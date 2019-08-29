package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// podHasVolumeClaim checks if the pods volume spec contains a
// PersistentVolumeClaim matching claimName.
func podHasVolumeClaim(pod *corev1.Pod, claimName string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		if volume.PersistentVolumeClaim.ClaimName == claimName {
			return true
		}
	}

	return false
}

// getStatefulSetLabel looks up the PersistentVolumeClaims StatefulSetLabel and
// returns it. The second return value denotes whether the label was found or
// not.
func getStatefulSetLabel(pvc *corev1.PersistentVolumeClaim) (string, bool) {
	if len(pvc.Labels) == 0 {
		return "", false
	}

	value, found := pvc.Labels[StatefulSetLabel]

	return value, found
}

// getStatefulSetLabelSelector creates a label selector which can be used to
// list PersistentVolumeClaims that have a StatefulSetLabel with name.
func getStatefulSetLabelSelector(name string) labels.Selector {
	set := labels.Set(map[string]string{
		StatefulSetLabel: name,
	})

	return labels.SelectorFromSet(set)
}

// isStatefulSetOwnerRef checks if ownerRef points to an apps/v1 StatefulSet.
func isStatefulSetOwnerRef(ownerRef *metav1.OwnerReference) bool {
	if ownerRef == nil {
		return false
	}

	return ownerRef.Kind == "StatefulSet" && ownerRef.APIVersion == "apps/v1"
}

// isMatchingSelector returns true if the labels of set are matching the
// selector.
func isMatchingSelector(set *appsv1.StatefulSet, selector labels.Selector) bool {
	return selector.Matches(labels.Set(set.Labels))
}
