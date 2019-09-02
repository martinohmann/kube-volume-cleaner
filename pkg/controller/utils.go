package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

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

// getVolumeClaimsWithAnnotation returns only PersistentVolumeClaims from the
// provided pvcs slice that have annotation with value.
func getVolumeClaimsWithAnnotation(pvcs []*corev1.PersistentVolumeClaim, annotation, value string) []*corev1.PersistentVolumeClaim {
	claims := make([]*corev1.PersistentVolumeClaim, 0)

	for _, pvc := range pvcs {
		v, exists := pvc.Annotations[annotation]
		if !exists || v != value {
			continue
		}

		claims = append(claims, pvc)
	}

	return claims
}

// getControlledPods returns only Pods from the provided pods slice that are
// controlled by set.
func getControlledPods(pods []*corev1.Pod, set *appsv1.StatefulSet) []*corev1.Pod {
	controlledPods := make([]*corev1.Pod, 0)

	for _, pod := range pods {
		if metav1.IsControlledBy(pod, set) {
			controlledPods = append(controlledPods, pod)
		}
	}

	return controlledPods
}

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

// getPodMountingVolumeClaim returns the Pod from the pods slice that mounts
// claimName. Returns nil if none of the pods is mounting claimName.
func getPodMountingVolumeClaim(pods []*corev1.Pod, claimName string) *corev1.Pod {
	for _, pod := range pods {
		if podHasVolumeClaim(pod, claimName) {
			return pod
		}
	}

	return nil
}
