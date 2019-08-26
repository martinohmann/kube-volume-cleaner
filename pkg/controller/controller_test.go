package controller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func fakeIndexerAdd(t *testing.T, c *Controller, objs ...runtime.Object) {
	for _, obj := range objs {
		switch o := obj.(type) {
		case *corev1.PersistentVolumeClaim:
			c.pvcInformer.GetIndexer().Add(o)
		case *corev1.Pod:
			c.podInformer.GetIndexer().Add(o)
		case *appsv1.StatefulSet:
			c.setInformer.GetIndexer().Add(o)
		default:
			t.Fatalf("unexpected object type %T", o)
		}
	}
}

func TestGetVolumeClaimsForStatefulSet(t *testing.T) {
	initialObjs := []runtime.Object{
		newPVCWithStatefulSetLabel("foo", "default", "the-statefulset"),
		newPVCWithStatefulSetLabel("bar", "default", "the-other-statefulset"),
		newPVCWithStatefulSetLabel("bar-foo", "default", "the-statefulset"),
		newPVCWithStatefulSetLabel("baz", "kube-system", "the-statefulset"),
		newPVC("qux", "default"),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	pvcs, err := c.getVolumeClaimsForStatefulSet("default", "the-statefulset")

	require.NoError(t, err)

	expected := []*corev1.PersistentVolumeClaim{
		newPVCWithStatefulSetLabel("foo", "default", "the-statefulset"),
		newPVCWithStatefulSetLabel("bar-foo", "default", "the-statefulset"),
	}

	assert.ElementsMatch(t, expected, pvcs)
}

func TestGetVolumeClaimsForPod(t *testing.T) {
	initialObjs := []runtime.Object{
		newPVC("foo", "default"),
		newPVC("bar", "default"),
		newPVC("baz", "kube-system"),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	pod := newPodWithVolumes("the-pod", "default", []corev1.Volume{
		{
			Name: "some-vol",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "foo",
				},
			},
		},
		{
			Name: "some-vol2",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "i-am-already-gone",
				},
			},
		},
		{
			Name: "other-vol",
		},
	})

	pvcs, err := c.getVolumeClaimsForPod(pod)

	require.NoError(t, err)

	expected := []*corev1.PersistentVolumeClaim{
		newPVC("foo", "default"),
	}

	assert.Equal(t, expected, pvcs)
}

func TestGetStatefulSet(t *testing.T) {
	initialObjs := []runtime.Object{
		newStatefulSet(1, "foo", "default"),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	set, err := c.getStatefulSet("default", "foo")

	require.NoError(t, err)

	assert.Equal(t, initialObjs[0], set)

	_, err = c.getStatefulSet("kube-system", "foo")

	require.Error(t, err)
}

func TestGetStatefulSetForPod(t *testing.T) {
	tests := []struct {
		name        string
		pod         *corev1.Pod
		expectError bool
		expected    *appsv1.StatefulSet
	}{
		{
			name: "ownerRef match, statefulset exists",
			pod: newPodWithOwnerRefs("foo", "default", []metav1.OwnerReference{
				newOwnerRef("the-set", "StatefulSet", "123"),
			}),
			expected: newStatefulSetWithUID(1, "the-set", "default", "123"),
		},
		{
			name: "ownerRef uid mismatch",
			pod: newPodWithOwnerRefs("foo", "default", []metav1.OwnerReference{
				newOwnerRef("the-set", "StatefulSet", "456"),
			}),
			expected: nil,
		},
		{
			name: "ownerRef kind mismatch",
			pod: newPodWithOwnerRefs("foo", "default", []metav1.OwnerReference{
				newOwnerRef("the-set", "DaemonSet", "123"),
			}),
			expected: nil,
		},
		{
			name: "ownerRef match, statefulset does not exist",
			pod: newPodWithOwnerRefs("foo", "default", []metav1.OwnerReference{
				newOwnerRef("other-set", "StatefulSet", "456"),
			}),
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := newStatefulSetWithUID(1, "the-set", "default", "123")

			c, err := newFakeController(set)

			fakeIndexerAdd(t, c, set)

			require.NoError(t, err)

			s, err := c.getStatefulSetForPod(test.pod)

			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expected, s)
			}
		})
	}
}

func TestGetPodForStatefulSet(t *testing.T) {
	initialObjs := []runtime.Object{
		newPodWithOwnerRefs("foo", "default", []metav1.OwnerReference{
			newOwnerRef("the-set", "StatefulSet", "123"),
		}),
		newPodWithOwnerRefs("foo", "kube-system", []metav1.OwnerReference{
			newOwnerRef("the-set", "StatefulSet", "123"),
		}),
		newPodWithOwnerRefs("bar", "default", []metav1.OwnerReference{
			newOwnerRef("the-set", "StatefulSet", "123"),
		}),
		newPodWithOwnerRefs("baz", "default", []metav1.OwnerReference{
			newOwnerRef("the-set", "StatefulSet", "456"),
		}),
		newPodWithOwnerRefs("qux", "default", []metav1.OwnerReference{
			newOwnerRef("other-set", "StatefulSet", "456"),
		}),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	set := newStatefulSetWithUID(1, "the-set", "default", "123")

	pods, err := c.getPodsForStatefulSet(set)

	require.NoError(t, err)

	expected := []*corev1.Pod{
		newPodWithOwnerRefs("bar", "default", []metav1.OwnerReference{
			newOwnerRef("the-set", "StatefulSet", "123"),
		}),
		newPodWithOwnerRefs("foo", "default", []metav1.OwnerReference{
			newOwnerRef("the-set", "StatefulSet", "123"),
		}),
	}

	assert.ElementsMatch(t, expected, pods)

	set = newStatefulSetWithUID(1, "the-set", "kube-public", "123")

	pods, err = c.getPodsForStatefulSet(set)

	require.NoError(t, err)
	assert.Len(t, pods, 0)
}

func TestUpdateStatefulSetLabel(t *testing.T) {
	pvc := newPVC("the-pvc", "the-ns")
	set := newStatefulSet(1, "the-statefulset", "the-ns")

	newLabel, oldLabel := updateStatefulSetLabel(pvc, set)

	assert.Empty(t, oldLabel)
	assert.Equal(t, "the-statefulset", newLabel)
	require.Len(t, pvc.Labels, 1)
	assert.Equal(t, "the-statefulset", pvc.Labels[StatefulSetLabel])

	newLabel, oldLabel = updateStatefulSetLabel(pvc, set)

	assert.Equal(t, "the-statefulset", oldLabel)
	assert.Equal(t, "the-statefulset", newLabel)

	set = newStatefulSet(1, "other-statefulset", "the-ns")

	newLabel, oldLabel = updateStatefulSetLabel(pvc, set)

	assert.Equal(t, "the-statefulset", oldLabel)
	assert.Equal(t, "other-statefulset", newLabel)
	require.Len(t, pvc.Labels, 1)
	assert.Equal(t, "other-statefulset", pvc.Labels[StatefulSetLabel])

	newLabel, oldLabel = updateStatefulSetLabel(pvc, nil)

	assert.Equal(t, "other-statefulset", oldLabel)
	assert.Empty(t, newLabel)
	require.Len(t, pvc.Labels, 0)

	newLabel, oldLabel = updateStatefulSetLabel(pvc, nil)

	assert.Empty(t, oldLabel)
	assert.Empty(t, newLabel)
	require.Len(t, pvc.Labels, 0)
}

func TestHandlePodUpdate(t *testing.T) {
	tests := []struct {
		name             string
		pod              *corev1.Pod
		initialObjs      []runtime.Object
		expectedQueueLen int
		expectError      bool
	}{
		{
			name: "pod update enqueues pvcs found in pod volumes",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithClaim("some-vol", "foo"),
			}),
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
				newPVC("bar", "default"),
				newPVC("bar", "kube-system"),
			},
			expectedQueueLen: 1,
		},
		{
			name: "pod update with deletion timestamp does not enqueue pvcs",
			pod: func() *corev1.Pod {
				pod := newPodWithVolumes("foo", "default", []corev1.Volume{
					newVolumeWithClaim("some-vol", "foo"),
				})
				pod.DeletionTimestamp = new(metav1.Time)
				return pod
			}(),
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
				newPVC("bar", "default"),
				newPVC("bar", "kube-system"),
			},
			expectedQueueLen: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := newFakeController(test.initialObjs...)

			require.NoError(t, err)

			fakeIndexerAdd(t, c, test.initialObjs...)

			err = c.handlePodUpdate(test.pod)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedQueueLen, c.pvcQueue.Len())
			}
		})
	}
}

func TestPodDeletionEnqueuesPVCsInNamespace(t *testing.T) {
	pvc1 := newPVC("foo", "default")
	pvc2 := newPVC("bar", "default")
	pvc3 := newPVC("bar", "kube-system")

	c, err := newFakeController(pvc1, pvc2, pvc3)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, pvc1, pvc2, pvc3)

	require.NoError(t, c.handlePodDeletion("default", "foo"))

	assert.Equal(t, 2, c.pvcQueue.Len())
}

func TestHandleStatefulSetUpdate(t *testing.T) {
	tests := []struct {
		name             string
		selector         string
		statefulSet      *appsv1.StatefulSet
		initialObjs      []runtime.Object
		expectedQueueLen int
		expectError      bool
	}{
		{
			name:        "statefulset update enqueues pods owned by statefulset",
			statefulSet: newStatefulSetWithUID(1, "foo", "default", "123"),
			initialObjs: []runtime.Object{
				newPodWithOwnerRefs("bar", "default", []metav1.OwnerReference{
					newOwnerRef("foo", "StatefulSet", "123"),
				}),
				newPod("qux", "default"),
			},
			expectedQueueLen: 1,
		},
		{
			name:        "only handles statefulsets matching the label selector",
			selector:    "foo=bar",
			statefulSet: newStatefulSetWithUID(1, "foo", "default", "123"),
			initialObjs: []runtime.Object{
				newPodWithOwnerRefs("bar", "default", []metav1.OwnerReference{
					newOwnerRef("foo", "StatefulSet", "123"),
				}),
				newPod("qux", "default"),
			},
			expectedQueueLen: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := newFakeControllerWithLabelSelector(test.selector, test.initialObjs...)

			require.NoError(t, err)

			fakeIndexerAdd(t, c, test.initialObjs...)

			err = c.handleStatefulSetUpdate(test.statefulSet)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedQueueLen, c.podQueue.Len())
			}
		})
	}
}

func TestStatefulSetDeletionEnqueuesLabeledPVCsInNamespace(t *testing.T) {
	initialObjs := []runtime.Object{
		newPVC("foo", "default"),
		newPVCWithStatefulSetLabel("bar", "default", "foo"),
		newPVCWithStatefulSetLabel("baz", "kube-system", "foo"),
		newPVC("bar", "kube-system"),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	require.NoError(t, c.handleStatefulSetDeletion("default", "foo"))

	assert.Equal(t, 1, c.pvcQueue.Len())
}

func TestPodHasVolumeClaim(t *testing.T) {
	tests := []struct {
		name      string
		pod       *corev1.Pod
		claimName string
		expected  bool
	}{
		{
			name: "pod with matching claim",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithClaim("some-vol", "bar"),
			}),
			claimName: "bar",
			expected:  true,
		},
		{
			name: "pod with other volumes and matching claim",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithHostPath("host-vol", "/var/run"),
				newVolumeWithClaim("some-vol", "bar"),
				newVolumeWithClaim("some-other-vol", "baz"),
			}),
			claimName: "bar",
			expected:  true,
		},
		{
			name: "pod without matching claim",
			pod: newPodWithVolumes("foo", "default", []corev1.Volume{
				newVolumeWithClaim("some-vol", "foo"),
			}),
			claimName: "bar",
			expected:  false,
		},
		{
			name:      "pod without volumes",
			pod:       newPod("foo", "default"),
			claimName: "bar",
			expected:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, podHasVolumeClaim(test.pod, test.claimName))
		})
	}
}

func newFakeController(initialObjects ...runtime.Object) (*Controller, error) {
	return newFakeControllerWithLabelSelectorAndNamespace("", metav1.NamespaceAll, initialObjects...)
}

func newFakeControllerWithNamespace(namespace string, initialObjects ...runtime.Object) (*Controller, error) {
	return newFakeControllerWithLabelSelectorAndNamespace("", namespace, initialObjects...)
}

func newFakeControllerWithLabelSelector(selector string, initialObjects ...runtime.Object) (*Controller, error) {
	return newFakeControllerWithLabelSelectorAndNamespace(selector, metav1.NamespaceAll, initialObjects...)
}

func newFakeControllerWithLabelSelectorAndNamespace(selector, namespace string, initialObjects ...runtime.Object) (*Controller, error) {
	client := fake.NewSimpleClientset(initialObjects...)

	return New(client, namespace, selector, false)
}

func newPod(name, namespace string) *corev1.Pod {
	return newPodWithVolumesAndOwnerRefs(name, namespace, nil, nil)
}

func newPodWithOwnerRefs(name, namespace string, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return newPodWithVolumesAndOwnerRefs(name, namespace, nil, ownerRefs)
}

func newPodWithVolumes(name, namespace string, volumes []corev1.Volume) *corev1.Pod {
	return newPodWithVolumesAndOwnerRefs(name, namespace, volumes, nil)
}

func newPodWithVolumesAndOwnerRefs(name, namespace string, volumes []corev1.Volume, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: ownerRefs,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
				},
			},
			Volumes: volumes,
		},
	}
}

func newPVC(name, namespace string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
				},
			},
		},
	}
}

func newPVCWithStatefulSetLabel(name, namespace, statefulSetName string) *corev1.PersistentVolumeClaim {
	pvc := newPVC(name, namespace)

	pvc.Labels = map[string]string{
		StatefulSetLabel: statefulSetName,
	}

	return pvc
}

func newStatefulSetWithVolumesAndUID(replicas int, name, namespace string, pvcMounts []corev1.VolumeMount, podMounts []corev1.VolumeMount, uid string) *appsv1.StatefulSet {
	mounts := append(pvcMounts, podMounts...)
	claims := []corev1.PersistentVolumeClaim{}
	for _, m := range pvcMounts {
		claims = append(claims, *newPVC(m.Name, "default"))
	}

	vols := []corev1.Volume{}
	for _, m := range podMounts {
		vols = append(vols, corev1.Volume{
			Name: m.Name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: fmt.Sprintf("/tmp/%v", m.Name),
				},
			},
		})
	}

	template := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:         "nginx",
					Image:        "nginx",
					VolumeMounts: mounts,
				},
			},
			Volumes: vols,
		},
	}

	template.Labels = map[string]string{"foo": "bar"}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"foo": "bar"},
			},
			Replicas: func() *int32 {
				i := int32(replicas)
				return &i
			}(),
			Template:             template,
			VolumeClaimTemplates: claims,
			ServiceName:          "governingsvc",
			UpdateStrategy:       appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			RevisionHistoryLimit: func() *int32 {
				limit := int32(2)
				return &limit
			}(),
		},
	}
}

func newStatefulSet(replicas int, name, namespace string) *appsv1.StatefulSet {
	pvcMounts := []corev1.VolumeMount{
		{Name: "datadir", MountPath: "/tmp/zookeeper"},
	}
	podMounts := []corev1.VolumeMount{
		{Name: "home", MountPath: "/home"},
	}
	return newStatefulSetWithVolumesAndUID(replicas, name, namespace, pvcMounts, podMounts, "")
}

func newStatefulSetWithUID(replicas int, name, namespace, uid string) *appsv1.StatefulSet {
	return newStatefulSetWithVolumesAndUID(replicas, name, namespace, nil, nil, uid)
}

func newOwnerRef(name, kind, uid string) metav1.OwnerReference {
	ctrl := true

	return metav1.OwnerReference{
		Name:       name,
		UID:        types.UID(uid),
		APIVersion: "apps/v1",
		Kind:       kind,
		Controller: &ctrl,
	}
}

func newVolumeWithClaim(name, claimName string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: claimName,
			},
		},
	}
}

func newVolumeWithHostPath(name, path string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: path,
			},
		},
	}
}
