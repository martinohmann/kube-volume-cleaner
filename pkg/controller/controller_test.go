package controller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
		newPVCWithStatefulSetAnnotation("foo", "default", "the-statefulset"),
		newPVCWithStatefulSetAnnotation("bar", "default", "the-other-statefulset"),
		newPVCWithStatefulSetAnnotation("bar-foo", "default", "the-statefulset"),
		newPVCWithStatefulSetAnnotation("baz", "kube-system", "the-statefulset"),
		newPVC("qux", "default"),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	pvcs, err := c.getVolumeClaimsForStatefulSet("default", "the-statefulset")

	require.NoError(t, err)

	expected := []*corev1.PersistentVolumeClaim{
		newPVCWithStatefulSetAnnotation("foo", "default", "the-statefulset"),
		newPVCWithStatefulSetAnnotation("bar-foo", "default", "the-statefulset"),
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

func TestSyncPod(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		initialObjs   []runtime.Object
		expectedError string
		prepare       func(t *testing.T, c *Controller)
		validate      func(t *testing.T, c *Controller)
	}{
		{
			name:          "invalid key",
			key:           "foo/bar/baz",
			expectedError: `unexpected key format: "foo/bar/baz"`,
		},
		{
			name: "deleted pod enqueues all pvcs in pod namespace",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
				newPVC("bar", "default"),
				newPVC("bar", "kube-system"),
			},
			validate: func(t *testing.T, c *Controller) {
				assert.Equal(t, 2, c.pvcQueue.Len())
			},
		},
		{
			name: "pod update enqueue mounted pvcs",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPodWithVolumes("foo", "default", []corev1.Volume{
					newVolumeWithClaim("some-vol", "foo"),
				}),
				newPVC("foo", "default"),
				newPVC("bar", "default"),
				newPVC("bar", "kube-system"),
			},
			validate: func(t *testing.T, c *Controller) {
				assert.Equal(t, 1, c.pvcQueue.Len())
			},
		},
		{
			name: "pod with deletion timestamp does not enqueue pvcs",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				func() *corev1.Pod {
					pod := newPodWithVolumes("foo", "default", []corev1.Volume{
						newVolumeWithClaim("some-vol", "foo"),
					})
					pod.DeletionTimestamp = new(metav1.Time)
					return pod
				}(),
				newPVC("foo", "default"),
				newPVC("bar", "default"),
				newPVC("bar", "kube-system"),
			},
			validate: func(t *testing.T, c *Controller) {
				assert.Equal(t, 0, c.pvcQueue.Len())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := newFakeController(test.initialObjs...)

			require.NoError(t, err)

			if test.prepare != nil {
				test.prepare(t, c)
			}

			fakeIndexerAdd(t, c, test.initialObjs...)

			err = c.syncPod(test.key)
			if test.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, test.expectedError, err.Error())
			} else {
				require.NoError(t, err)
			}

			if test.validate != nil {
				test.validate(t, c)
			}
		})
	}
}

func TestSyncStatefulSet(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		initialObjs   []runtime.Object
		expectedError string
		prepare       func(t *testing.T, c *Controller)
		validate      func(t *testing.T, c *Controller)
	}{
		{
			name:          "invalid key",
			key:           "foo/bar/baz",
			expectedError: `unexpected key format: "foo/bar/baz"`,
		},
		{
			name: "statefulset update enqueues pods owned by statefulset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newStatefulSetWithUID(1, "foo", "default", "123"),
				newPodWithOwnerRefs("bar", "default", []metav1.OwnerReference{
					newOwnerRef("foo", "StatefulSet", "123"),
				}),
				newPod("qux", "default"),
			},
			validate: func(t *testing.T, c *Controller) {
				assert.Equal(t, 1, c.podQueue.Len())
			},
		},
		{
			name: "no pods owned by statefulset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newStatefulSetWithUID(1, "foo", "default", "123"),
				newPodWithOwnerRefs("bar", "default", []metav1.OwnerReference{
					newOwnerRef("foo", "StatefulSet", "456"),
				}),
				newPod("qux", "default"),
			},
			validate: func(t *testing.T, c *Controller) {
				assert.Equal(t, 0, c.podQueue.Len())
			},
		},
		{
			name: "statefulset deletion enqueues annotated pvcs in namespace",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
				newPVCWithStatefulSetAnnotation("bar", "default", "foo"),
				newPVCWithStatefulSetAnnotation("baz", "kube-system", "foo"),
				newPVC("bar", "kube-system"),
			},
			validate: func(t *testing.T, c *Controller) {
				assert.Equal(t, 1, c.pvcQueue.Len())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := newFakeController(test.initialObjs...)

			require.NoError(t, err)

			if test.prepare != nil {
				test.prepare(t, c)
			}

			fakeIndexerAdd(t, c, test.initialObjs...)

			err = c.syncStatefulSet(test.key)
			if test.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, test.expectedError, err.Error())
			} else {
				require.NoError(t, err)
			}

			if test.validate != nil {
				test.validate(t, c)
			}
		})
	}
}

func TestSyncVolumeClaim(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		initialObjs   []runtime.Object
		expectedError string
		prepare       func(t *testing.T, c *Controller)
		validate      func(t *testing.T, c *Controller)
	}{
		{
			name:          "invalid key",
			key:           "foo/bar/baz",
			expectedError: `unexpected key format: "foo/bar/baz"`,
		},
		{
			name: "deleted pvc does not result in error",
			key:  "default/foo",
		},
		{
			name: "pvc with deletion timestamp is not processed again",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				func() *corev1.PersistentVolumeClaim {
					pvc := newPVC("foo", "default")
					pvc.DeletionTimestamp = new(metav1.Time)
					return pvc
				}(),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
				require.NotNil(t, pvc)

				assert.Len(t, pvc.Annotations, 0)
			},
		},
		{
			name: "remove annotation if pod mounting pvc is not part of statefulset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
				newPodWithVolumes("bar", "default", []corev1.Volume{
					newVolumeWithClaim("vol", "foo"),
				}),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Len(t, pvc.Annotations, 0)
			},
		},
		{
			name: "add annotation pvc is managed by statefulset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
				newPodWithVolumesAndOwnerRefs(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
					[]metav1.OwnerReference{newOwnerRef("the-set", "StatefulSet", "123")},
				),
				newStatefulSetWithUID(1, "the-set", "default", "123"),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Equal(t, "the-set", pvc.Annotations[StatefulSetAnnotation])
			},
		},
		{
			name: "update annotation if statefulset for pvc changed",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
				newPodWithVolumesAndOwnerRefs(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
					[]metav1.OwnerReference{newOwnerRef("baz", "StatefulSet", "123")},
				),
				newStatefulSetWithUID(1, "baz", "default", "123"),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Equal(t, "baz", pvc.Annotations[StatefulSetAnnotation])
			},
		},
		{
			name: "remove annotation if statefulset does not match label selector",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
				newPodWithVolumesAndOwnerRefs(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
					[]metav1.OwnerReference{newOwnerRef("the-set", "StatefulSet", "123")},
				),
				newStatefulSetWithUID(1, "the-set", "default", "123"),
			},
			prepare: func(t *testing.T, c *Controller) {
				selector, _ := labels.Parse("foo=bar")
				c.labelSelector = selector
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Len(t, pvc.Annotations, 0)
			},
		},
		{
			name: "remove annotation if statefulset does not match label selector (2)",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
				newStatefulSetWithUID(1, "the-set", "default", "123"),
			},
			prepare: func(t *testing.T, c *Controller) {
				selector, _ := labels.Parse("foo=bar")
				c.labelSelector = selector
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Len(t, pvc.Annotations, 0)
			},
		},
		{
			name: "pod references unknown statefulset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
				newPodWithVolumesAndOwnerRefs(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
					[]metav1.OwnerReference{newOwnerRef("the-set", "StatefulSet", "123")},
				),
			},
			expectedError: `statefulset.apps "the-set" not found`,
		},
		{
			name: "delete pvc with annotation if not mounted and statefulset deleted",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(err))
			},
		},
		{
			name: "do not delete pvc with annotation if not mounted and statefulset deleted in dry run mode",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				c.dryRun = true
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "do not delete pvc if annotation not present",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "do not delete pvc if statefulset is still present",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
				newStatefulSetWithUID(1, "the-set", "default", "123"),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Equal(t, "the-set", pvc.Annotations[StatefulSetAnnotation])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := newFakeController(test.initialObjs...)

			require.NoError(t, err)

			if test.prepare != nil {
				test.prepare(t, c)
			}

			fakeIndexerAdd(t, c, test.initialObjs...)

			err = c.syncVolumeClaim(test.key)
			if test.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, test.expectedError, err.Error())
			} else {
				require.NoError(t, err)
			}

			if test.validate != nil {
				test.validate(t, c)
			}
		})
	}
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

func newPVCWithStatefulSetAnnotation(name, namespace, statefulSetName string) *corev1.PersistentVolumeClaim {
	pvc := newPVC(name, namespace)

	pvc.Annotations = map[string]string{
		StatefulSetAnnotation: statefulSetName,
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
