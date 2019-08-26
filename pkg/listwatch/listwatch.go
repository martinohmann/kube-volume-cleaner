package listwatch

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// NewListWatchFromClient creates a new ListWatch from the specified client, resource, namespace and label selector.
func NewListWatchFromClient(c cache.Getter, resource string, namespace string, labelSelector labels.Selector) *cache.ListWatch {
	optionsModifier := func(options *metav1.ListOptions) {
		options.LabelSelector = labelSelector.String()
	}
	return cache.NewFilteredListWatchFromClient(c, resource, namespace, optionsModifier)
}

func NewPodListWatch(client kubernetes.Interface, namespace string, labelSelector labels.Selector) *cache.ListWatch {
	return NewListWatchFromClient(client.CoreV1().RESTClient(), "pods", namespace, labelSelector)
}

func NewPersistentVolumeClaimListWatch(client kubernetes.Interface, namespace string, labelSelector labels.Selector) *cache.ListWatch {
	return NewListWatchFromClient(client.CoreV1().RESTClient(), "persistentvolumeclaims", namespace, labelSelector)
}

func NewStatefulSetListWatch(client kubernetes.Interface, namespace string, labelSelector labels.Selector) *cache.ListWatch {
	return NewListWatchFromClient(client.AppsV1().RESTClient(), "statefulsets", namespace, labelSelector)
}
