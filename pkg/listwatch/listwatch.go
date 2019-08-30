package listwatch

import (
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

func NewPodListWatcher(client kubernetes.Interface, namespace string) *cache.ListWatch {
	return cache.NewListWatchFromClient(client.CoreV1().RESTClient(), "pods", namespace, fields.Everything())
}

func NewPersistentVolumeClaimListWatcher(client kubernetes.Interface, namespace string) *cache.ListWatch {
	return cache.NewListWatchFromClient(client.CoreV1().RESTClient(), "persistentvolumeclaims", namespace, fields.Everything())
}

func NewStatefulSetListWatcher(client kubernetes.Interface, namespace string) *cache.ListWatch {
	return cache.NewListWatchFromClient(client.AppsV1().RESTClient(), "statefulsets", namespace, fields.Everything())
}
