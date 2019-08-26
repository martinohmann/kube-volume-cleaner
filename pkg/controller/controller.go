package controller

import (
	"time"

	"k8s.io/klog"

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
	StatefulSetLabel = "statefulset.kube-volume-cleaner.io/managed-by"
)

type Controller struct {
	client kubernetes.Interface

	podQueue workqueue.Interface
	pvcQueue workqueue.Interface
	setQueue workqueue.Interface

	namespace     string
	labelSelector labels.Selector
	dryRun        bool

	podInformer cache.SharedIndexInformer
	podLister   corev1listers.PodLister

	pvcInformer cache.SharedIndexInformer
	pvcLister   corev1listers.PersistentVolumeClaimLister

	setInformer cache.SharedIndexInformer
	setLister   appsv1listers.StatefulSetLister
}

func New(client kubernetes.Interface, namespace, selector string, dryRun bool) (*Controller, error) {
	labelSelector, err := labels.Parse(selector)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse label selector")
	}

	podListWatcher := listwatch.NewPodListWatch(client, namespace, labelSelector)
	podInformer := cache.NewSharedIndexInformer(podListWatcher, &corev1.Pod{}, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podLister := corev1listers.NewPodLister(podInformer.GetIndexer())

	pvcListWatcher := listwatch.NewPersistentVolumeClaimListWatch(client, namespace, labels.Everything())
	pvcInformer := cache.NewSharedIndexInformer(pvcListWatcher, &corev1.PersistentVolumeClaim{}, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcLister := corev1listers.NewPersistentVolumeClaimLister(pvcInformer.GetIndexer())

	setListWatcher := listwatch.NewStatefulSetListWatch(client, namespace, labels.Everything())
	setInformer := cache.NewSharedIndexInformer(setListWatcher, &appsv1.StatefulSet{}, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	setLister := appsv1listers.NewStatefulSetLister(setInformer.GetIndexer())

	c := &Controller{
		client:        client,
		podInformer:   podInformer,
		podLister:     podLister,
		podQueue:      workqueue.NewNamed("pod"),
		pvcInformer:   pvcInformer,
		pvcLister:     pvcLister,
		pvcQueue:      workqueue.NewNamed("pvc"),
		setInformer:   setInformer,
		setLister:     setLister,
		setQueue:      workqueue.NewNamed("statefulset"),
		namespace:     namespace,
		labelSelector: labelSelector,
		dryRun:        dryRun,
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
	c.enqueue(c.podQueue, obj)
}

func (c *Controller) enqueuePersistentVolumeClaim(obj interface{}) {
	c.enqueue(c.pvcQueue, obj)
}

func (c *Controller) enqueueStatefulSet(obj interface{}) {
	c.enqueue(c.setQueue, obj)
}

func (c *Controller) enqueue(queue workqueue.Interface, obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("failed to get key from object: %v", err)
		return
	}

	queue.Add(key)

	klog.V(5).Infof("enqueued %q for sync", key)
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

	go wait.Until(worker(c.podQueue, c.syncPod), time.Second, stopCh)
	go wait.Until(worker(c.pvcQueue, c.syncVolumeClaim), time.Second, stopCh)
	go wait.Until(worker(c.setQueue, c.syncStatefulSet), time.Second, stopCh)

	<-stopCh
	klog.Info("stopping controller")
}

func worker(queue workqueue.Interface, syncFunc func(string) error) func() {
	return func() {
		workFunc := func() bool {
			key, quit := queue.Get()
			if quit {
				return true
			}
			defer queue.Done(key)

			err := syncFunc(key.(string))
			utilruntime.HandleError(err)
			return false
		}

		for {
			if quit := workFunc(); quit {
				return
			}
		}
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
		klog.V(4).Infof("pod %s/%s is in status pending deletion, not handling", pod.Namespace, pod.Name)
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
	if apierrors.IsNotFound(err) {
		klog.V(4).Infof("no pvcs in pod namespace %q, nothing to do", namespace)
		return nil
	}

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
		klog.V(4).Infof("pvc %s/%s is in status pending deletion, not handling", pvc.Namespace, pvc.Name)
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

	if pod == nil {
		// pvc is not mounted anymore. If it has a statefulset label and the
		// statefulset does not exist anymore, it is safe to delete.
		klog.V(4).Infof("pvc %s/%s is not mounted to a pod", pvc.Namespace, pvc.Name)

		setName, exists := getStatefulSetLabel(pvc)
		if !exists {
			// do we need to do something here?
			klog.V(4).Infof("pvc %s/%s does not have label %s, not candidate for deletion", pvc.Namespace, pvc.Name, StatefulSetLabel)
			return nil
		}

		_, err := c.getStatefulSet(pvc.Namespace, setName)
		if err == nil {
			klog.V(4).Infof("statefulset managing pvc %s/%s still present, not deleting pvc", pvc.Namespace, pvc.Name)
			return nil
		}

		if !apierrors.IsNotFound(err) {
			klog.V(1).Infof("failed to retrieve statefulset for pvc %s/%s", pvc.Namespace, pvc.Name)
			return err
		}

		// we are safe to delete the pvc.
		if c.dryRun {
			klog.Infof("would delete pvc %s/%s, but dry run is enabled", pvc.Namespace, pvc.Name)
			return nil
		}

		klog.Infof("deleting pvc %s/%s", pvc.Namespace, pvc.Name)

		return c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(pvc.Name, &metav1.DeleteOptions{})
	}

	set, err := c.getStatefulSetForPod(pod)
	if apierrors.IsNotFound(err) {
		klog.V(4).Infof("pod %s/%s is not managed by a statefulset, ignoring", pod.Namespace, pod.Name)
		return nil
	}

	if err != nil {
		klog.V(1).Infof("failed to retrieve statefulset for pod %s/%s", pod.Namespace, pod.Name)
		return err
	}

	return c.updateStatefulSetLabelOnVolumeClaim(set, pvc)
}

func (c *Controller) updateStatefulSetLabelOnVolumeClaim(set *appsv1.StatefulSet, pvc *corev1.PersistentVolumeClaim) error {
	pvcCopy := pvc.DeepCopy()

	newLabel, oldLabel := updateStatefulSetLabel(pvcCopy, set)
	if newLabel == oldLabel {
		klog.V(4).Infof("no label change needed on pvc %s/%s", pvc.Namespace, pvc.Name)
		return nil
	}

	klog.Infof("updating label %q on pvc %s/%s: %q -> %q", StatefulSetLabel, pvc.Namespace, pvc.Name, oldLabel, newLabel)

	_, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvcCopy)

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
	if !c.labelSelector.Matches(labels.Set(set.Labels)) {
		klog.V(4).Infof("statefulset %s/%s does not match label selector %q, ignoring", set.Namespace, set.Name, c.labelSelector.String())
		return nil
	}

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

func (c *Controller) getPodForVolumeClaim(pvc *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	pods, err := c.podLister.Pods(pvc.Namespace).List(labels.Everything())
	if apierrors.IsNotFound(err) {
		klog.V(4).Infof("pvc %s/%s is not mounted to any pod", pvc.Namespace, pvc.Name)
		return nil, nil
	}

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

func (c *Controller) getPodsForStatefulSet(set *appsv1.StatefulSet) ([]*corev1.Pod, error) {
	pods, err := c.podLister.Pods(set.Namespace).List(labels.Everything())
	if apierrors.IsNotFound(err) {
		return []*corev1.Pod{}, nil
	}

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

func (c *Controller) getStatefulSet(namespace, name string) (*appsv1.StatefulSet, error) {
	return c.setLister.StatefulSets(namespace).Get(name)
}

func (c *Controller) getStatefulSetForPod(pod *corev1.Pod) (*appsv1.StatefulSet, error) {
	ownerRef := metav1.GetControllerOf(pod)
	if !isStatefulSetOwnerRef(ownerRef) {
		return nil, nil
	}

	set, err := c.setLister.StatefulSets(pod.Namespace).Get(ownerRef.Name)
	if err != nil {
		return nil, err
	}

	if metav1.IsControlledBy(pod, set) {
		return set, nil
	}

	return nil, nil
}

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

func (c *Controller) getVolumeClaimsForStatefulSet(namespace, name string) ([]*corev1.PersistentVolumeClaim, error) {
	selector := getStatefulSetLabelSelector(name)

	return c.pvcLister.PersistentVolumeClaims(namespace).List(selector)
}

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

func getStatefulSetLabel(pvc *corev1.PersistentVolumeClaim) (string, bool) {
	if len(pvc.Labels) == 0 {
		return "", false
	}

	value, found := pvc.Labels[StatefulSetLabel]

	return value, found
}

func getStatefulSetLabelSelector(name string) labels.Selector {
	set := labels.Set(map[string]string{
		StatefulSetLabel: name,
	})

	return labels.SelectorFromSet(set)
}

func updateStatefulSetLabel(pvc *corev1.PersistentVolumeClaim, set *appsv1.StatefulSet) (string, string) {
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}

	oldLabel, isLabeled := pvc.Labels[StatefulSetLabel]

	if set == nil && isLabeled {
		delete(pvc.Labels, StatefulSetLabel)
		return "", oldLabel
	}

	if set != nil && (!isLabeled || oldLabel != set.Name) {
		pvc.Labels[StatefulSetLabel] = set.Name
		return set.Name, oldLabel
	}

	return oldLabel, oldLabel
}

func isStatefulSetOwnerRef(ownerRef *metav1.OwnerReference) bool {
	if ownerRef == nil {
		return false
	}

	return ownerRef.Kind == "StatefulSet" && ownerRef.APIVersion == "apps/v1"
}
