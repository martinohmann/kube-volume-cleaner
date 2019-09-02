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

	// DeleteAfterAnnotation is added to PersistentVolumeClaims that should be
	// deleted only after the specified moment in time. The annotation is set
	// once the managing StatefulSet and the Pod mounting the
	// PersistentVolumeClaim are deleted and the controller is configured to
	// run with a delete-after value > 0s.
	DeleteAfterAnnotation = "pvc.kube-volume-cleaner.io/delete-after"

	// maxSyncAttempts is the number of attempts to make to sync a given object
	// key before giving up.
	maxSyncAttempts = 5
)

// Controller watches for PersistentVolumeClaims that where created by a
// StatefulSet and will delete them once the StatefulSet and its Pods are
// deleted.
type Controller struct {
	client kubernetes.Interface

	labelSelector  labels.Selector
	namespace      string
	noDelete       bool
	deleteAfter    time.Duration
	resyncInterval time.Duration

	podQueue workqueue.RateLimitingInterface
	pvcQueue workqueue.RateLimitingInterface
	setQueue workqueue.RateLimitingInterface

	podLister corev1listers.PodLister
	pvcLister corev1listers.PersistentVolumeClaimLister
	setLister appsv1listers.StatefulSetLister

	podInformer cache.SharedIndexInformer
	pvcInformer cache.SharedIndexInformer
	setInformer cache.SharedIndexInformer
}

// New creates a new *Controller with client and options. Will return an error
// if options contain malformed values.
func New(client kubernetes.Interface, options *config.Options) (*Controller, error) {
	labelSelector, err := labels.Parse(options.LabelSelector)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse label selector")
	}

	indexers := cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}

	podListWatcher := listwatch.NewPodListWatcher(client, options.Namespace)
	pvcListWatcher := listwatch.NewPersistentVolumeClaimListWatcher(client, options.Namespace)
	setListWatcher := listwatch.NewStatefulSetListWatcher(client, options.Namespace)

	podInformer := cache.NewSharedIndexInformer(podListWatcher, &corev1.Pod{}, 0, indexers)
	pvcInformer := cache.NewSharedIndexInformer(pvcListWatcher, &corev1.PersistentVolumeClaim{}, 0, indexers)
	setInformer := cache.NewSharedIndexInformer(setListWatcher, &appsv1.StatefulSet{}, 0, indexers)

	c := &Controller{
		client:         client,
		labelSelector:  labelSelector,
		namespace:      options.Namespace,
		noDelete:       options.NoDelete,
		deleteAfter:    options.DeleteAfter,
		resyncInterval: options.ResyncInterval,
		podInformer:    podInformer,
		pvcInformer:    pvcInformer,
		setInformer:    setInformer,
		podLister:      corev1listers.NewPodLister(podInformer.GetIndexer()),
		pvcLister:      corev1listers.NewPersistentVolumeClaimLister(pvcInformer.GetIndexer()),
		setLister:      appsv1listers.NewStatefulSetLister(setInformer.GetIndexer()),
		podQueue:       workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		pvcQueue:       workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		setQueue:       workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
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

// Run starts the controller and will block until stopCh is closed.
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

	go wait.Until(c.resync, c.resyncInterval, stopCh)
	go wait.Until(worker(c.podQueue, c.syncPod, "pod"), time.Second, stopCh)
	go wait.Until(worker(c.pvcQueue, c.syncVolumeClaim, "pvc"), time.Second, stopCh)
	go wait.Until(worker(c.setQueue, c.syncStatefulSet, "statefulset"), time.Second, stopCh)

	<-stopCh
	klog.Info("stopping controller")
}

// worker returns a function that can be used as a worker to consume queue and
// call syncFunc for every key obtained from the queue. Kind just denotes the
// object kind to add some context to log messages.
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

// handleError ensures that key gets requeued for a number of times in case err
// is non-nil until it gives up and logs the final error. The value of kind is
// just used to add some context about the type of the object that key points
// to and is only used in log messages.
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

// resync resyncs the controller by enqueuing all PersistentVolumeClaims that
// are available via the informer. It is expected to call resync periodically.
func (c *Controller) resync() {
	klog.V(4).Info("resyncing pvcs")

	pvcs, err := c.pvcLister.PersistentVolumeClaims(c.namespace).List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, pvc := range pvcs {
		c.enqueuePersistentVolumeClaim(pvc)
	}
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
	} else if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve pod %q from informer", key)
		return err
	}

	return c.handlePodDeletion(namespace, name)
}

func (c *Controller) handlePodUpdate(pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil {
		// On a restart of the controller, it's possible a new pod shows up in
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
	} else if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve pvc %q from informer", key)
		return err
	}

	return c.handleVolumeClaimDeletion(namespace, name)
}

// handleVolumeClaimUpdate contains the core logic of the controller and takes
// decisions about whether and when a PersistentVolumeClaim should be deleted.
func (c *Controller) handleVolumeClaimUpdate(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.DeletionTimestamp != nil {
		// On a restart of the controller, it's possible a new pvc shows up in
		// a state that is already pending deletion. Prevent the pvc from being
		// a creation/update observation.
		klog.V(5).Infof("pvc %s/%s is in status pending deletion, not handling", pvc.Namespace, pvc.Name)
		return nil
	}

	pod, err := c.getPodForVolumeClaim(pvc)
	if err != nil {
		klog.V(1).Infof("error while getting pod for pvc %s/%s", pvc.Namespace, pvc.Name)
		return err
	}

	if pod != nil {
		set, err := c.getStatefulSetForPod(pod)
		if apierrors.IsNotFound(err) {
			// This indicates that the pod has an ownerRef that points to a
			// non-existent statefulset. This can happen during deletion of a
			// statefulset when the local caches are not aware of the updated pods.
			// We should NOT try to remove the statefulset annotation from pvc
			// here, otherwise we will end up with pvcs not being deleted although
			// they should be.
			return nil
		} else if err != nil {
			klog.V(1).Infof("failed to retrieve statefulset for pod %s/%s", pod.Namespace, pod.Name)
			return err
		}

		return c.syncVolumeClaimAnnotations(pvc, set)
	}

	klog.V(5).Infof("pvc %s/%s is not mounted to a pod", pvc.Namespace, pvc.Name)

	setName, exists := pvc.Annotations[StatefulSetAnnotation]
	if !exists {
		klog.V(5).Infof("pvc %s/%s does not have annotation %s, no candidate for deletion", pvc.Namespace, pvc.Name, StatefulSetAnnotation)
		return nil
	}

	set, err := c.setLister.StatefulSets(pvc.Namespace).Get(setName)
	if err == nil {
		if isMatchingSelector(set, c.labelSelector) {
			klog.V(5).Infof("statefulset %s/%s managing pvc %s/%s still present, not deleting pvc", set.Namespace, set.Name, pvc.Namespace, pvc.Name)
			return nil
		}

		klog.V(5).Infof("statefulset %s/%s managing pvc %s/%s does not match label selector %q", set.Namespace, set.Name, pvc.Namespace, pvc.Name, c.labelSelector.String())
		return c.removeVolumeClaimAnnotations(pvc)
	} else if !apierrors.IsNotFound(err) {
		klog.V(1).Infof("failed to retrieve statefulset for pvc %s/%s", pvc.Namespace, pvc.Name)
		return err
	}

	deleteAfter := c.getDeleteAfter(pvc)

	now := time.Now()

	if deleteAfter.After(now) {
		return c.updateDeleteAfterAnnotation(pvc, deleteAfter)
	}

	ok, err := c.isVolumeClaimSafeToDelete(pvc.Name, pvc.Namespace)
	if err != nil || !ok {
		return err
	}

	// The pvc is not mounted anymore, has a statefulset annotation and the
	// annotated statefulset is deleted. We are safe to delete the pvc.
	return c.deleteVolumeClaim(pvc)
}

// getDeleteAfter returns the time encoded in the DeleteAfterAnnotation of pvc,
// if it exists, or a new time determined by the delete-after configuration
// value if it did not exist or was unparsable (e.g. because somebody tampered
// with it).
func (c *Controller) getDeleteAfter(pvc *corev1.PersistentVolumeClaim) time.Time {
	value, found := pvc.Annotations[DeleteAfterAnnotation]
	if !found {
		return time.Now().Add(c.deleteAfter)
	}

	deleteAfter, err := time.Parse(time.RFC3339, value)
	if err != nil {
		klog.V(5).Infof("failed to parse %q annotation: %v", DeleteAfterAnnotation, err)
		deleteAfter = time.Now().Add(c.deleteAfter)
	}

	return deleteAfter
}

// isVolumeClaimSafeToDelete checks if all preconditions for deleting a PVC are
// met by querying the api server for live data. This ensures that we do not
// make wrong decisions based on stale data from cache.
func (c *Controller) isVolumeClaimSafeToDelete(name, namespace string) (bool, error) {
	pvc, err := c.client.CoreV1().PersistentVolumeClaims(namespace).Get(name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}

	podList, err := c.client.CoreV1().Pods(pvc.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return false, err
	}

	var pod *corev1.Pod

	for _, p := range podList.Items {
		if podHasVolumeClaim(&p, pvc.Name) {
			pod = &p
			break
		}
	}

	if pod != nil {
		return false, nil
	}

	setName, exists := pvc.Annotations[StatefulSetAnnotation]
	if !exists {
		return false, nil
	}

	_, err = c.client.AppsV1().StatefulSets(pvc.Namespace).Get(setName, metav1.GetOptions{})
	if err == nil {
		return false, nil
	} else if !apierrors.IsNotFound(err) {
		return false, err
	}

	return !c.getDeleteAfter(pvc).After(time.Now()), nil
}

// syncVolumeClaimAnnotations synchronizes pvc annotations managed by the
// controller. The passed in StatefulSet must be the one managing the pvc or
// nil if there is none. This function will update the StatefulSetAnnotation on
// pvc with the StatefulSets name. In case the StatefulSet is nil or does not
// match the configured label selector, it is ensured that this the
// StatefulSetAnnotation gets removed from the pvc.
// If we get here there is no way that the pvc should be scheduled for deletion
// by the controller. Thus, any change of the StatefulSetAnnotation is paired
// with the removal of the potentially existant DeleteAfterAnnotation from the
// pvc to ensure scheduled deletions are aborted.
func (c *Controller) syncVolumeClaimAnnotations(pvc *corev1.PersistentVolumeClaim, set *appsv1.StatefulSet) error {
	switch {
	case set == nil:
		klog.V(4).Infof("pod mounting pvc %s/%s does not belong to statefulset", pvc.Namespace, pvc.Name)
		return c.removeVolumeClaimAnnotations(pvc)
	case !isMatchingSelector(set, c.labelSelector):
		klog.V(5).Infof("statefulset %s/%s managing pvc %s/%s does not match label selector %q", set.Namespace, set.Name, pvc.Namespace, pvc.Name, c.labelSelector.String())
		return c.removeVolumeClaimAnnotations(pvc)
	default:
		return c.updateStatefulSetAnnotation(pvc, set.Name)
	}
}

// removeVolumeClaimAnnotations removes all annotations managed by this
// controller from pvc and syncs the updates to the apiserver if the pvc object
// changed.
func (c *Controller) removeVolumeClaimAnnotations(pvc *corev1.PersistentVolumeClaim) error {
	pvcCopy := pvc.DeepCopy()

	objChanged := removeVolumeClaimAnnotations(pvcCopy, StatefulSetAnnotation, DeleteAfterAnnotation)
	if !objChanged {
		return nil
	}

	return c.updateVolumeClaim(pvcCopy)
}

// updateStatefulSetAnnotation sets the StatefulSetAnnotation of pvc to
// newValue if it does not already have it. It also ensures that
// DeleteAfterAnnotation is remove if present, because if we got here there is
// no way the pvc is eligible for deletion. If the pvc objects changes due to
// the annotation updates, it gets synced to the apiserver afterwards.
func (c *Controller) updateStatefulSetAnnotation(pvc *corev1.PersistentVolumeClaim, newValue string) error {
	pvcCopy := pvc.DeepCopy()

	removed := removeVolumeClaimAnnotations(pvcCopy, DeleteAfterAnnotation)

	updated := updateVolumeClaimAnnotation(pvcCopy, StatefulSetAnnotation, newValue)
	if !removed && !updated {
		return nil
	}

	return c.updateVolumeClaim(pvcCopy)
}

// updateDeleteAfterAnnotation sets the DeleteAfterAnnotation to the RFC3339
// string representation of newTime, and syncs the object to the apiserver if
// the annotation value changed.
func (c *Controller) updateDeleteAfterAnnotation(pvc *corev1.PersistentVolumeClaim, newTime time.Time) error {
	newValue := newTime.Format(time.RFC3339)
	pvcCopy := pvc.DeepCopy()

	updated := updateVolumeClaimAnnotation(pvcCopy, DeleteAfterAnnotation, newValue)
	if !updated {
		return nil
	}

	return c.updateVolumeClaim(pvcCopy)
}

// deleteVolumeClaim deletes a PersistentVolumeClaim from the cluster. If the
// controller runs in no-delete mode, the deletion operation is just printed
// but not executed.
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

// updateVolumeClaim syncs a pvc update to the apiserver.
func (c *Controller) updateVolumeClaim(pvc *corev1.PersistentVolumeClaim) error {
	klog.V(5).Infof("updating pvc %s/%s", pvc.Namespace, pvc.Name)

	_, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)

	return err
}

// handleVolumeClaimDeletion handles PersistentVolumeClaim deletion events
// emitted by the informer.
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
	} else if !apierrors.IsNotFound(err) {
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
	} else if err != nil {
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

	return getPodMountingVolumeClaim(pods, pvc.Name), nil
}

// getPodsForStatefulSet retrieves all pods from the informer that are
// controlled by set.
func (c *Controller) getPodsForStatefulSet(set *appsv1.StatefulSet) ([]*corev1.Pod, error) {
	pods, err := c.podLister.Pods(set.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	return getControlledPods(pods, set), nil
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
	} else if err != nil {
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
		} else if err != nil {
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

	return getVolumeClaimsWithAnnotation(pvcs, StatefulSetAnnotation, name), nil
}
