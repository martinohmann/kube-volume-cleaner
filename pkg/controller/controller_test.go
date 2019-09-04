package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/martinohmann/kube-volume-cleaner/pkg/config"
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
	clienttesting "k8s.io/client-go/testing"
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

func TestGetStatefulSetForPod(t *testing.T) {
	tests := []struct {
		name          string
		pod           *corev1.Pod
		expectedError string
		expected      *appsv1.StatefulSet
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
			expectedError: `statefulset.apps "other-set" not found`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := newStatefulSetWithUID(1, "the-set", "default", "123")

			c, err := newFakeController(set)

			fakeIndexerAdd(t, c, set)

			require.NoError(t, err)

			s, err := c.getStatefulSetForPod(test.pod)

			if test.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, test.expectedError, err.Error())
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
			name: "remove annotations if pod mounting pvc is not part of statefulset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithAnnotations("foo", "default", map[string]string{
					StatefulSetAnnotation: "the-set",
					ControllerAnnotation:  config.DefaultControllerID,
				}),
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
			name: "do not remove annotations if pvc is managed by another controller",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithAnnotations("foo", "default", map[string]string{
					StatefulSetAnnotation: "the-set",
					ControllerAnnotation:  "the-other-controller",
				}),
				newPodWithVolumes("bar", "default", []corev1.Volume{
					newVolumeWithClaim("vol", "foo"),
				}),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Len(t, pvc.Annotations, 2)
				assert.Equal(t, "the-other-controller", pvc.Annotations[ControllerAnnotation])
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
				assert.Equal(t, config.DefaultControllerID, pvc.Annotations[ControllerAnnotation])
			},
		},
		{
			name: "remove deleteAfter annotation if pvc is mounted by pod",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				func() *corev1.PersistentVolumeClaim {
					pvc := newPVCWithStatefulSetAnnotation("foo", "default", "the-set")
					pvc.Annotations[DeleteAfterAnnotation] = time.Now().Add(24 * time.Hour).Format(time.RFC3339)
					return pvc
				}(),
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

				_, present := pvc.Annotations[DeleteAfterAnnotation]

				assert.Equal(t, "the-set", pvc.Annotations[StatefulSetAnnotation])
				assert.False(t, present)
			},
		},
		{
			name: "do not update annotation if statefulset for pvc stays the same",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithAnnotations("foo", "default", map[string]string{
					StatefulSetAnnotation: "the-set",
					ControllerAnnotation:  config.DefaultControllerID,
				}),
				newPodWithVolumesAndOwnerRefs(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
					[]metav1.OwnerReference{newOwnerRef("the-set", "StatefulSet", "123")},
				),
				newStatefulSetWithUID(1, "the-set", "default", "123"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("update", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					t.Fatalf("unexpected pvc update")
					return true, nil, nil
				})
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Equal(t, "the-set", pvc.Annotations[StatefulSetAnnotation])
			},
		},
		{
			name: "do not sync pvc update to apiserver if there is no annotation to delete",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVC("foo", "default"),
				newPodWithVolumes(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
				),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("update", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					t.Fatalf("unexpected pvc update")
					return true, nil, nil
				})
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
				newPVC("foo", "default"),
				newPodWithVolumesAndOwnerRefs(
					"bar",
					"default",
					[]corev1.Volume{newVolumeWithClaim("vol", "foo")},
					[]metav1.OwnerReference{newOwnerRef("the-set", "StatefulSet", "123")},
				),
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.Len(t, pvc.Annotations, 0)
			},
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
			name: "deletion attempt of already deleted pvc does not cause error",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("*", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					deleteAction := action.(clienttesting.DeleteAction)
					gvr := deleteAction.GetResource()
					return true, nil, apierrors.NewNotFound(gvr.GroupResource(), deleteAction.GetName())
				})
			},
		},
		{
			name: "do not delete pvc with annotation if not mounted and statefulset deleted in no-delete mode",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				c.noDelete = true
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
		{
			name: "mark pvc for deletion if deleteAfter > 0",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				c.deleteAfter = 2 * time.Hour
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.NotEmpty(t, pvc.Annotations[DeleteAfterAnnotation])

				deleteAfter := c.getDeleteAfter(pvc)

				assert.True(t, deleteAfter.After(time.Now()))
			},
		},
		{
			name: "delete pvc if value in deleteAfter annotation is surpassed",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				func() *corev1.PersistentVolumeClaim {
					pvc := newPVCWithStatefulSetAnnotation("foo", "default", "the-set")
					pvc.Annotations[DeleteAfterAnnotation] = time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
					return pvc
				}(),
			},
			prepare: func(t *testing.T, c *Controller) {
				c.deleteAfter = 2 * time.Hour
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(err))
			},
		},
		{
			name: "do not update deleteAfter annotation if it is already present",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				func() *corev1.PersistentVolumeClaim {
					pvc := newPVCWithStatefulSetAnnotation("foo", "default", "the-set")
					pvc.Annotations[DeleteAfterAnnotation] = time.Now().Add(24 * time.Hour).Format(time.RFC3339)
					return pvc
				}(),
			},
			prepare: func(t *testing.T, c *Controller) {
				c.deleteAfter = 2 * time.Hour

				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("update", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					t.Fatalf("unexpected pvc update")
					return true, nil, nil
				})
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "invalid deleteAfter annotation will be reset",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				func() *corev1.PersistentVolumeClaim {
					pvc := newPVCWithStatefulSetAnnotation("foo", "default", "the-set")
					pvc.Annotations[DeleteAfterAnnotation] = "foobar"
					return pvc
				}(),
			},
			prepare: func(t *testing.T, c *Controller) {
				c.deleteAfter = 2 * time.Hour
			},
			validate: func(t *testing.T, c *Controller) {
				pvc, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)

				assert.NotEmpty(t, pvc.Annotations[DeleteAfterAnnotation])

				deleteAfter := c.getDeleteAfter(pvc)

				assert.True(t, deleteAfter.After(time.Now()))
			},
		},
		{
			name: "cache lists statefulset as deleted, but it is present in api server => do not delete pvc",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("get", "statefulsets", func(action clienttesting.Action) (bool, runtime.Object, error) {
					getAction := action.(clienttesting.GetAction)

					set := newStatefulSetWithUID(1, getAction.GetName(), getAction.GetNamespace(), "123")

					return true, set, nil
				})
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "cache has pvc as not mounted by a pod, but api server says otherwise => do not delete pvc",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("list", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					listAction := action.(clienttesting.ListAction)

					pod := newPodWithVolumes("bar", listAction.GetNamespace(), []corev1.Volume{
						newVolumeWithClaim("vol", "foo"),
					})

					podList := &corev1.PodList{
						Items: []corev1.Pod{*pod},
					}

					return true, podList, nil
				})
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "error occured while getting pvc from apiserver => do not delete pvc",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewUnauthorized("not authorized")
				})
				fakeClient.PrependReactor("delete", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					t.Fatalf("unexpected delete operation")
					return true, nil, nil
				})
			},
			expectedError: "not authorized",
		},
		{
			name: "error occured while listing pods on apiserver => do not delete pvc",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("list", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewUnauthorized("not authorized")
				})
			},
			expectedError: "not authorized",
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "error occured while getting statefulset from apiserver => do not delete pvc",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("get", "statefulsets", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewUnauthorized("not authorized")
				})
			},
			expectedError: "not authorized",
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "pvc from apiserver does not have statefulset annotation => do not delete pvc",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithStatefulSetAnnotation("foo", "default", "the-set"),
			},
			prepare: func(t *testing.T, c *Controller) {
				fakeClient := c.client.(*fake.Clientset)
				fakeClient.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					getAction := action.(clienttesting.GetAction)

					pvc := newPVC(getAction.GetName(), getAction.GetNamespace())
					return true, pvc, nil
				})
				fakeClient.PrependReactor("delete", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
					t.Fatalf("unexpected delete operation")
					return true, nil, nil
				})
			},
		},
		{
			name: "do not delete pvc if it is owned by another controller",
			key:  "default/foo",
			initialObjs: []runtime.Object{
				newPVCWithAnnotations("foo", "default", map[string]string{
					StatefulSetAnnotation: "the-set",
					ControllerAnnotation:  "foo",
				}),
			},
			validate: func(t *testing.T, c *Controller) {
				_, err := c.client.CoreV1().PersistentVolumeClaims("default").Get("foo", metav1.GetOptions{})
				require.NoError(t, err)
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

func TestResync(t *testing.T) {
	initialObjs := []runtime.Object{
		newPVC("foo", "default"),
		newPVC("bar", "kube-system"),
	}

	c, err := newFakeController(initialObjs...)

	require.NoError(t, err)

	fakeIndexerAdd(t, c, initialObjs...)

	c.resync()

	assert.Equal(t, 2, c.pvcQueue.Len())
}

func newFakeController(initialObjects ...runtime.Object) (*Controller, error) {
	return newFakeControllerWithLabelSelectorAndNamespace("", metav1.NamespaceAll, initialObjects...)
}

func newFakeControllerWithLabelSelectorAndNamespace(selector, namespace string, initialObjects ...runtime.Object) (*Controller, error) {
	client := fake.NewSimpleClientset(initialObjects...)

	return New(client, &config.Options{LabelSelector: selector, Namespace: namespace, ControllerID: config.DefaultControllerID})
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
	annotations := map[string]string{
		StatefulSetAnnotation: statefulSetName,
	}

	return newPVCWithAnnotations(name, namespace, annotations)
}

func newPVCWithAnnotations(name, namespace string, annotations map[string]string) *corev1.PersistentVolumeClaim {
	pvc := newPVC(name, namespace)

	pvc.Annotations = annotations

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
