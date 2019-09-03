package controller

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

// removeVolumeClaimAnnotation removes annotation from the pvc object. The
// return value denotes whether the object was modified or not.
func removeVolumeClaimAnnotation(pvc *corev1.PersistentVolumeClaim, annotation string) bool {
	if !metav1.HasAnnotation(pvc.ObjectMeta, annotation) {
		return false
	}

	klog.Infof("removing annotation %q from pvc %s/%s", annotation, pvc.Namespace, pvc.Name)
	delete(pvc.Annotations, annotation)

	return true
}

// removeVolumeClaimAnnotations removes annotations from the pvc object. The
// return value denotes whether the object was modified or not.
func removeVolumeClaimAnnotations(pvc *corev1.PersistentVolumeClaim, annotations ...string) bool {
	objChanged := false

	for _, annotation := range annotations {
		if removeVolumeClaimAnnotation(pvc, annotation) {
			objChanged = true
		}
	}

	return objChanged
}

// updateVolumeClaimAnnotation updates annotation with newValue on the pvc
// object. The return value denotes wether the object was modified or not.
func updateVolumeClaimAnnotation(pvc *corev1.PersistentVolumeClaim, annotation, newValue string) bool {
	oldValue := pvc.Annotations[annotation]

	if newValue == oldValue {
		return false
	}

	klog.Infof(`updating pvc %s/%s annotation "%s=%s"`, pvc.Namespace, pvc.Name, annotation, newValue)

	metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, annotation, newValue)

	return true
}
