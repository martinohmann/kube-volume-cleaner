package controller

import (
	"time"

	"k8s.io/klog"

	"github.com/martinohmann/kube-volume-cleaner/pkg/config"
	"github.com/martinohmann/kube-volume-cleaner/pkg/listwatch"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	// StatefulSetAnnotation is added to PersistentVolumeClaims that the
	// controller identifies as managed by a StatefulSet. The value is always
	// the name of the managing StatefulSet in the same namespace as the
	// PersistentVolumeClaim. The annotation is used to identify
	// PersistentVolumeClaims after the deletion of a StatefulSet.
	StatefulSetAnnotation = "statefulset.kube-volume-cleaner.io/managed-by"

	// maxSyncAttempts is the number of attempts to make to sync a given object
	// key before giving up.
	maxSyncAttempts = 5
)

type Controller struct {
	client kubernetes.Interface

	podQueue workqueue.RateLimitingInterface
	pvcQueue workqueue.RateLimitingInterface
	setQueue workqueue.RateLimitingInterface

	labelSelector labels.Selector
	noDelete      bool

	podInformer cache.SharedIndexInformer
	podLister   corev1listers.PodLister

	pvcInformer cache.SharedIndexInformer
	pvcLister   corev1listers.PersistentVolumeClaimLister

	setInformer cache.SharedIndexInformer
	setLister   appsv1listers.StatefulSetLister
}

func New(client kubernetes.Interface, options *config.Options) (*Controller, error) {
	labelSelector, err := labels.Parse(options.LabelSelector)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse label selector")
	}

	indexers := cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}

	podListWatcher := listwatch.NewPodListWatcher(client, options.Namespace)
	podInformer := cache.NewSharedIndexInformer(podListWatcher, &corev1.Pod{}, 0, indexers)

	pvcListWatcher := listwatch.NewPersistentVolumeClaimListWatcher(client, options.Namespace)
	pvcInformer := cache.NewSharedIndexInformer(pvcListWatcher, &corev1.PersistentVolumeClaim{}, 0, indexers)

	setListWatcher := listwatch.NewStatefulSetListWatcher(client, options.Namespace)
	setInformer := cache.NewSharedIndexInformer(setListWatcher, &appsv1.StatefulSet{}, 0, indexers)

	c := &Controller{
		client:        client,
		podInformer:   podInformer,
		podLister:     corev1listers.NewPodLister(podInformer.GetIndexer()),
		podQueue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		pvcInformer:   pvcInformer,
		pvcLister:     corev1listers.NewPersistentVolumeClaimLister(pvcInformer.GetIndexer()),
		pvcQueue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		setInformer:   setInformer,
		setLister:     appsv1listers.NewStatefulSetLister(setInformer.GetIndexer()),
		setQueue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		labelSelector: labelSelector,
		noDelete:      options.NoDelete,
	}

	c.registerEventHandlers()

	return c, nil
}

func (c *Controller) registerEventHandlers() {
	c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueuePod,
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueuePod(newObj)
		},
		DeleteFunc: c.enqueuePod,
	})

	c.pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueuePersistentVolumeClaim,
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueuePersistentVolumeClaim(newObj)
		},
		DeleteFunc: c.enqueuePersistentVolumeClaim,
	})

	c.setInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueStatefulSet,
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueStatefulSet(newObj)
		},
		DeleteFunc: c.enqueueStatefulSet,
	})
}

func (c *Controller) enqueuePod(obj interface{}) {
	c.enqueue(c.podQueue, obj, "pod")
}

func (c *Controller) enqueuePersistentVolumeClaim(obj interface{}) {
	c.enqueue(c.pvcQueue, obj, "pvc")
}

func (c *Controller) enqueueStatefulSet(obj interface{}) {
	c.enqueue(c.setQueue, obj, "statefulset")
}

func (c *Controller) enqueue(queue workqueue.Interface, obj interface{}, kind string) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("failed to get key from object: %v", err)
		return
	}

	queue.Add(key)

	klog.V(5).Infof("enqueued %s %q for sync", kind, key)
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	defer c.podQueue.ShutDown()
	defer c.pvcQueue.ShutDown()
	defer c.setQueue.ShutDown()

	klog.Info("starting controller")

	go c.podInformer.Run(stopCh)
	go c.pvcInformer.Run(stopCh)
	go c.setInformer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.podInformer.HasSynced, c.pvcInformer.HasSynced, c.setInformer.HasSynced) {
		utilruntime.HandleError(errors.Errorf("timed out waiting for caches to sync"))
		return
	}

	go wait.Until(worker(c.podQueue, c.syncPod, "pod"), time.Second, stopCh)
	go wait.Until(worker(c.pvcQueue, c.syncVolumeClaim, "pvc"), time.Second, stopCh)
	go wait.Until(worker(c.setQueue, c.syncStatefulSet, "statefulset"), time.Second, stopCh)

	<-stopCh
	klog.Info("stopping controller")
}

func worker(queue workqueue.RateLimitingInterface, syncFunc func(string) error, kind string) func() {
	return func() {
		workFunc := func() bool {
			key, quit := queue.Get()
			if quit {
				return true
			}
			defer queue.Done(key)

			err := syncFunc(key.(string))
			handleError(queue, err, key, kind)
			return false
		}

		for {
			if quit := workFunc(); quit {
				return
			}
		}
	}
}

func handleError(queue workqueue.RateLimitingInterface, err error, key interface{}, kind string) {
	if err == nil {
		queue.Forget(key)
		return
	}

	if queue.NumRequeues(key) < maxSyncAttempts {
		klog.V(4).Infof("requeuing %s key for sync %v due to error: %v", kind, key, err)
		queue.AddRateLimited(key)
		return
	}

	queue.Forget(key)
	utilruntime.HandleError(err)
	klog.V(1).Infof("dropping key %s %v out of the queue due to error: %v", kind, key, err)
}

func (c *Controller) syncPod(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.V(1).Infof("error parsing key %q to get pod from informer", key)
		return err
	}

	pod, err := c.podLister.Pods(namespace).Get(name)
	if err == nil {
		return c.handlePodUpdate(pod)
	}

	if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve pod %q from informer", key)
		return err
	}

	return c.handlePodDeletion(namespace, name)
}

func (c *Controller) handlePodUpdate(pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil {
		// on a restart of the controller, it's possible a new pod shows up in
		// a state that is already pending deletion. Prevent the pod from being
		// a creation/update observation.
		klog.V(5).Infof("pod %s/%s is in status pending deletion, not handling", pod.Namespace, pod.Name)
		return nil
	}

	pvcs, err := c.getVolumeClaimsForPod(pod)
	if err != nil {
		klog.V(1).Infof("failed to retrieve pvcs for pod %s/%s", pod.Namespace, pod.Name)
		return err
	}

	for _, pvc := range pvcs {
		c.enqueuePersistentVolumeClaim(pvc)
	}

	return nil
}

func (c *Controller) handlePodDeletion(namespace, name string) error {
	klog.V(4).Infof("pod %s/%s deleted", namespace, name)

	pvcs, err := c.pvcLister.PersistentVolumeClaims(namespace).List(labels.Everything())
	if err != nil {
		klog.V(1).Infof("failed to retrieve pvcs for namespace %q", namespace)
		return err
	}

	for _, pvc := range pvcs {
		c.enqueuePersistentVolumeClaim(pvc)
	}

	return nil
}

func (c *Controller) syncVolumeClaim(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.V(1).Infof("error parsing key %q to get pvc from informer", key)
		return err
	}

	pvc, err := c.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	if err == nil {
		return c.handleVolumeClaimUpdate(pvc)
	}

	if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve pvc %q from informer", key)
		return err
	}

	return c.handleVolumeClaimDeletion(namespace, name)
}

func (c *Controller) handleVolumeClaimUpdate(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.DeletionTimestamp != nil {
		// on a restart of the controller, it's possible a new pvc shows up in
		// a state that is already pending deletion. Prevent the pod from being
		// a creation/update observation.
		klog.V(5).Infof("pvc %s/%s is in status pending deletion, not handling", pvc.Namespace, pvc.Name)
		return nil
	}

	// TODO(mohmann): when the information from the informer cache tells us
	// that it is safe to delete the pvc, check again against api-server to
	// doublecheck before attempting deletion
	pod, err := c.getPodForVolumeClaim(pvc)
	if err != nil {
		klog.V(1).Infof("error while getting pod for pvc %s/%s", pvc.Namespace, pvc.Name)
		return err
	}

	if pod != nil {
		return c.updateVolumeClaimAnnotations(pvc, pod)
	}

	klog.V(5).Infof("pvc %s/%s is not mounted to a pod, checking if it should be deleted", pvc.Namespace, pvc.Name)

	setName, exists := getStatefulSetAnnotation(pvc)
	if !exists {
		// do we need to do something here?
		klog.V(5).Infof("pvc %s/%s does not have annotation %s, no candidate for deletion", pvc.Namespace, pvc.Name, StatefulSetAnnotation)
		return nil
	}

	set, err := c.getStatefulSet(pvc.Namespace, setName)
	if err == nil {
		if isMatchingSelector(set, c.labelSelector) {
			klog.V(5).Infof("statefulset %s/%s managing pvc %s/%s still present, not deleting pvc", set.Namespace, set.Name, pvc.Namespace, pvc.Name)
			return nil
		}

		klog.V(5).Infof("statefulset %s/%s managing pvc %s/%s does not match label selector %q", set.Namespace, set.Name, pvc.Namespace, pvc.Name, c.labelSelector.String())
		return c.removeStatefulSetAnnotation(pvc)
	}

	if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve statefulset for pvc %s/%s", pvc.Namespace, pvc.Name)
		return err
	}

	// The pvc is not mounted anymore, has a statefulset annotation and the
	// annotated statefulset is deleted. We are safe to delete the pvc.
	return c.deleteVolumeClaim(pvc)
}

func (c *Controller) updateVolumeClaimAnnotations(pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod) error {
	set, err := c.getStatefulSetForPod(pod)

	switch {
	case apierrors.IsNotFound(err):
		// This indicates that the pod has an ownerRef that points to a
		// non-existent statefulset. This can happen during deletion of a
		// statefulset when the local caches are not aware of the updated pods.
		// We should NOT try to remove the statefulset annotation form pvc
		// here, otherwise we will end up with pvcs not being deleted although
		// they should be.
		return nil
	case err != nil:
		klog.V(1).Infof("failed to retrieve statefulset for pod %s/%s", pod.Namespace, pod.Name)
		return err
	case set == nil:
		klog.V(4).Infof("pod mounting pvc %s/%s does not belong to statefulset", pvc.Namespace, pvc.Name)
		return c.removeStatefulSetAnnotation(pvc)
	case !isMatchingSelector(set, c.labelSelector):
		klog.V(5).Infof("statefulset %s/%s controlling pod %s/%s does not match label selector %q", set.Namespace, set.Name, pod.Namespace, pod.Name, c.labelSelector.String())
		return c.removeStatefulSetAnnotation(pvc)
	}

	return c.updateStatefulSetAnnotation(pvc, set.Name)
}

func (c *Controller) removeStatefulSetAnnotation(pvc *corev1.PersistentVolumeClaim) error {
	_, found := getStatefulSetAnnotation(pvc)
	if !found {
		return nil
	}

	pvcCopy := pvc.DeepCopy()

	delete(pvcCopy.Annotations, StatefulSetAnnotation)

	klog.Infof("removing annotation %q from pvc %s/%s", StatefulSetAnnotation, pvc.Namespace, pvc.Name)

	return c.updateVolumeClaim(pvcCopy)
}

func (c *Controller) updateStatefulSetAnnotation(pvc *corev1.PersistentVolumeClaim, newValue string) error {
	pvcCopy := pvc.DeepCopy()

	oldValue, exists := getStatefulSetAnnotation(pvcCopy)
	if !exists && pvcCopy.Annotations == nil {
		pvcCopy.Annotations = map[string]string{}
	}

	pvcCopy.Annotations[StatefulSetAnnotation] = newValue
	if newValue == oldValue {
		return nil
	}

	if oldValue == "" {
		klog.Infof("adding annotation %q on pvc %s/%s: %q", StatefulSetAnnotation, pvc.Namespace, pvc.Name, newValue)
	} else {
		klog.Infof("updating annotation %q on pvc %s/%s: %q -> %q", StatefulSetAnnotation, pvc.Namespace, pvc.Name, oldValue, newValue)
	}

	return c.updateVolumeClaim(pvcCopy)
}

func (c *Controller) deleteVolumeClaim(pvc *corev1.PersistentVolumeClaim) error {
	if c.noDelete {
		klog.Infof("would delete pvc %s/%s, but no-delete mode is enabled", pvc.Namespace, pvc.Name)
		return nil
	}

	klog.Infof("deleting pvc %s/%s", pvc.Namespace, pvc.Name)

	err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(pvc.Name, &metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		klog.V(5).Infof("pvc %s/%s vanished while attempting to delete it", pvc.Namespace, pvc.Name)
		return nil
	}

	return err
}

func (c *Controller) updateVolumeClaim(pvc *corev1.PersistentVolumeClaim) error {
	klog.V(5).Infof("updating pvc %s/%s", pvc.Namespace, pvc.Name)

	_, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)

	return err
}

func (c *Controller) handleVolumeClaimDeletion(namespace, name string) error {
	// Deletions are actually what we want, so there is nothing to do here. We
	// just log the event anyways for debugging.
	klog.V(4).Infof("pvc %s/%s deleted", namespace, name)

	return nil
}

func (c *Controller) syncStatefulSet(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.V(1).Infof("error parsing key %q to get statefulset from informer", key)
		return err
	}

	set, err := c.setLister.StatefulSets(namespace).Get(name)
	if err == nil {
		return c.handleStatefulSetUpdate(set)
	}

	if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve statefulset %q from informer", key)
		return err
	}

	return c.handleStatefulSetDeletion(namespace, name)
}

func (c *Controller) handleStatefulSetUpdate(set *appsv1.StatefulSet) error {
	pods, err := c.getPodsForStatefulSet(set)
	if err != nil {
		klog.V(1).Infof("failed to retrieve pods for statefulset %s/%s", set.Namespace, set.Name)
		return err
	}

	for _, pod := range pods {
		c.enqueuePod(pod)
	}

	return nil
}

func (c *Controller) handleStatefulSetDeletion(namespace, name string) error {
	klog.V(4).Infof("statefulset %s/%s deleted", namespace, name)

	pvcs, err := c.getVolumeClaimsForStatefulSet(namespace, name)
	if apierrors.IsNotFound(err) {
		klog.V(4).Infof("statefulset %s/%s does not have any pvcs", namespace, name)
		return nil
	}

	if err != nil {
		return err
	}

	for _, pvc := range pvcs {
		c.enqueuePersistentVolumeClaim(pvc)
	}

	return nil
}

// getPodForVolumeClaim retrieves the pod currently mounting pvc from the
// informer. If pvc is not mounted to any pod, both return values will be nil.
func (c *Controller) getPodForVolumeClaim(pvc *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	pods, err := c.podLister.Pods(pvc.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, pod := range pods {
		if podHasVolumeClaim(pod, pvc.Name) {
			return pod, nil
		}
	}

	return nil, nil
}

// getPodsForStatefulSet retrieves all pods from the informer that are
// controlled by set.
func (c *Controller) getPodsForStatefulSet(set *appsv1.StatefulSet) ([]*corev1.Pod, error) {
	pods, err := c.podLister.Pods(set.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	controlledPods := make([]*corev1.Pod, 0)
	for _, pod := range pods {
		if metav1.IsControlledBy(pod, set) {
			controlledPods = append(controlledPods, pod)
		}
	}

	return controlledPods, nil
}

// getStatefulSet retrieves the StatefulSet with name and namespace from the
// informer.
func (c *Controller) getStatefulSet(namespace, name string) (*appsv1.StatefulSet, error) {
	return c.setLister.StatefulSets(namespace).Get(name)
}

// getStatefulSetForPod finds that StatefulSet owning pod in the informer. If
// the pod is not owned by a StatefulSet, both return values will be nil.
func (c *Controller) getStatefulSetForPod(pod *corev1.Pod) (*appsv1.StatefulSet, error) {
	ownerRef := metav1.GetControllerOf(pod)
	if !isStatefulSetOwnerRef(ownerRef) {
		return nil, nil
	}

	set, err := c.setLister.StatefulSets(pod.Namespace).Get(ownerRef.Name)
	if apierrors.IsNotFound(err) {
		klog.V(5).Infof("pod %s/%s ownerRef points to statefulset %s/%s but it was not found", pod.Namespace, pod.Name, pod.Namespace, ownerRef.Name)
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if metav1.IsControlledBy(pod, set) {
		return set, nil
	}

	return nil, nil
}

// getVolumeClaimsForPod retrieves all PersistentVolumeClaims that are
// referenced in the pods volume spec from the informer. References pointing to
// PersistentVolumeClaims that cannot be found are ignored and do not cause
// errors.
func (c *Controller) getVolumeClaimsForPod(pod *corev1.Pod) ([]*corev1.PersistentVolumeClaim, error) {
	pvcs := make([]*corev1.PersistentVolumeClaim, 0)

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		claimName := volume.PersistentVolumeClaim.ClaimName

		pvc, err := c.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(claimName)
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		pvcs = append(pvcs, pvc)
	}

	return pvcs, nil
}

// getVolumeClaimsForStatefulSet finds all PersistentVolumeClaims in the
// informer that are annotated with the StatefulSets name in the same
// namespace.
func (c *Controller) getVolumeClaimsForStatefulSet(namespace, name string) ([]*corev1.PersistentVolumeClaim, error) {
	pvcs, err := c.pvcLister.PersistentVolumeClaims(namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	managedVolumeClaims := make([]*corev1.PersistentVolumeClaim, 0)

	for _, pvc := range pvcs {
		value, exists := getStatefulSetAnnotation(pvc)
		if !exists || value != name {
			continue
		}

		managedVolumeClaims = append(managedVolumeClaims, pvc)
	}

	return managedVolumeClaims, nil
}
