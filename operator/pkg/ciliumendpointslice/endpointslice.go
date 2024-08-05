// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package ciliumendpointslice

import (
	"context"
	"fmt"
	"time"

	"github.com/cilium/hive/cell"
	"github.com/cilium/workerpool"
	"github.com/sirupsen/logrus"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/workqueue"

	"github.com/cilium/cilium/pkg/k8s"
	cilium_api_v2 "github.com/cilium/cilium/pkg/k8s/apis/cilium.io/v2"
	capi_v2a1 "github.com/cilium/cilium/pkg/k8s/apis/cilium.io/v2alpha1"
	"github.com/cilium/cilium/pkg/k8s/resource"
	"github.com/cilium/cilium/pkg/logging/logfields"
)

const (
	// cesNamePrefix is the prefix name added for the CiliumEndpointSlice
	// resource.
	cesNamePrefix = "ces"

	// defaultSyncBackOff is the default backoff period for cesSync calls.
	defaultSyncBackOff = 1 * time.Second
	// maxSyncBackOff is the max backoff period for cesSync calls.
	maxSyncBackOff = 100 * time.Second
	// maxRetries is the number of times a cesSync will be retried before it is
	// dropped out of the queue.
	maxRetries = 15

	// deprecatedIdentityMode is the old name of `identity` mode. It is kept for backwards
	// compatibility but will be removed in the future.
	deprecatedIdentityMode = "cesSliceModeIdentity"
	// deprecatedFcfsMode is the old name of `fcfs` mode. It is kept for backwards
	// compatibility but will be removed in the future.
	deprecatedFcfsMode = "cesSliceModeFCFS"
	// CEPs are batched into a CES, based on its Identity
	identityMode = "identity"
	// CEPs are inserted into the largest, non-empty CiliumEndpointSlice
	fcfsMode = "fcfs"

	// Default CES Synctime, multiple consecutive syncs with k8s-apiserver are
	// batched and synced together after a short delay.
	DefaultCESSyncTime = 500 * time.Millisecond

	CESWriteQPSLimitMax = 50
	CESWriteQPSBurstMax = 100
)

func (c *Controller) initializeQueue() {
	c.logger.WithFields(logrus.Fields{
		logfields.WorkQueueQPSLimit:    c.rateLimit.current.Limit,
		logfields.WorkQueueBurstLimit:  c.rateLimit.current.Burst,
		logfields.WorkQueueSyncBackOff: defaultSyncBackOff,
	}).Info("CES controller workqueue configuration")
	c.rateLimiter = workqueue.NewItemExponentialFailureRateLimiter(defaultSyncBackOff, maxSyncBackOff)
	c.fastQueue = workqueue.NewRateLimitingQueueWithConfig(
		c.rateLimiter,
		workqueue.RateLimitingQueueConfig{Name: "cilium_endpoint_slice"})
	c.standardQueue = workqueue.NewRateLimitingQueueWithConfig(
		c.rateLimiter,
		workqueue.RateLimitingQueueConfig{Name: "cilium_endpoint_slice"})
}

func (c *Controller) onEndpointUpdate(cep *cilium_api_v2.CiliumEndpoint) {
	if cep.Status.Networking == nil || cep.Status.Identity == nil || cep.GetName() == "" || cep.Namespace == "" {
		return
	}
	c.logger.Infof("on CEP update %s, %s", cep.Name, cep.Namespace)
	touchedCESs := c.manager.UpdateCEPMapping(k8s.ConvertCEPToCoreCEP(cep), cep.Namespace)
	c.enqueueCESReconciliation(touchedCESs)
}

func (c *Controller) onEndpointDelete(cep *cilium_api_v2.CiliumEndpoint) {
	c.logger.Infof("on CEP delete %s, %s", cep.Name, cep.Namespace)
	touchedCES := c.manager.RemoveCEPMapping(k8s.ConvertCEPToCoreCEP(cep), cep.Namespace)
	c.enqueueCESReconciliation([]CESName{touchedCES})
}

func (c *Controller) onSliceUpdate(ces *capi_v2a1.CiliumEndpointSlice) {
	c.logger.Infof("on CES update %s, %s", ces.Name, ces.Namespace)
	c.enqueueCESReconciliation([]CESName{NewCESNameNamespace(ces.Name, ces.Namespace)})
}

func (c *Controller) onSliceDelete(ces *capi_v2a1.CiliumEndpointSlice) {
	c.logger.Infof("on CES delete %s, %s", ces.Name, ces.Namespace)
	c.enqueueCESReconciliation([]CESName{NewCESNameNamespace(ces.Name, ces.Namespace)})
}

func (c *Controller) addToQueue(ces CESName) {
	_, ok := c.priorityNamespaces[ces.Namespace]

	if ok {
		time.AfterFunc(c.defaultCESSyncTime, func() {
			c.logger.Infof("CES %s, %s added to the fast queue after %d", ces.Name, ces.Namespace, c.defaultCESSyncTime)
			c.fastQueue.Add(ces)
			c.cond.Signal()
		})
		//c.fastQueue.AddAfter(ces, c.defaultCESSyncTime)

	} else {
		time.AfterFunc(c.defaultCESSyncTime, func() {
			c.logger.Infof("CES %s, %s added to the stadard queue after %d", ces.Name, ces.Namespace, c.defaultCESSyncTime)
			c.standardQueue.Add(ces)
			c.cond.Signal()
		})
		//c.standardQueue.AddAfter(ces, c.defaultCESSyncTime)

	}
	//time.AfterFunc(c.defaultCESSyncTime, c.cond.Signal)
}

func (c *Controller) enqueueCESReconciliation(cess []CESName) {
	for _, ces := range cess {
		c.logger.WithFields(logrus.Fields{
			logfields.CESName: ces.string(),
		}).Debug("Enqueueing CES (if not empty name)")
		if ces.Name != "" {
			c.enqueuedAtLock.Lock()
			if c.enqueuedAt[ces].IsZero() {
				c.enqueuedAt[ces] = time.Now()
			}
			c.enqueuedAtLock.Unlock()
			c.addToQueue(ces)
		}
	}
}

func (c *Controller) getAndResetCESProcessingDelay(ces CESName) float64 {
	c.enqueuedAtLock.Lock()
	defer c.enqueuedAtLock.Unlock()
	enqueued, exists := c.enqueuedAt[ces]
	if !exists {
		return 0
	}
	if !enqueued.IsZero() {
		delay := time.Since(enqueued)
		c.enqueuedAt[ces] = time.Time{}
		return delay.Seconds()
	}
	return 0
}

// start the worker thread, reconciles the modified CESs with api-server
func (c *Controller) Start(ctx cell.HookContext) error {
	c.logger.Info("Bootstrap ces controller")
	c.context, c.contextCancel = context.WithCancel(context.Background())
	defer utilruntime.HandleCrash()

	switch c.slicingMode {
	case identityMode, deprecatedIdentityMode:
		if c.slicingMode == deprecatedIdentityMode {
			c.logger.Warnf("%v is deprecated and has been renamed. Please use %v instead", deprecatedIdentityMode, identityMode)
		}
		c.manager = newCESManagerIdentity(c.maxCEPsInCES, c.logger)

	case fcfsMode, deprecatedFcfsMode:
		if c.slicingMode == deprecatedFcfsMode {
			c.logger.Warnf("%v is deprecated and has been renamed. Please use %v instead", deprecatedFcfsMode, fcfsMode)
		}
		c.manager = newCESManagerFcfs(c.maxCEPsInCES, c.logger)

	default:
		return fmt.Errorf("Invalid slicing mode: %s", c.slicingMode)
	}

	c.reconciler = newReconciler(c.context, c.clientset.CiliumV2alpha1(), c.manager, c.logger, c.ciliumEndpoint, c.ciliumEndpointSlice, c.metrics)

	c.initializeQueue()

	if err := c.syncCESsInLocalCache(ctx); err != nil {
		return err
	}

	// Start the work pools processing CEP events only after syncing CES in local cache.
	c.wp = workerpool.New(3)
	c.wp.Submit("cilium-endpoints-updater", c.runCiliumEndpointsUpdater)
	c.wp.Submit("cilium-endpoint-slices-updater", c.runCiliumEndpointSliceUpdater)
	c.wp.Submit("cilium-nodes-updater", c.runCiliumNodesUpdater)

	c.logger.Info("Starting CES controller reconciler.")
	go c.worker()

	return nil
}

func (c *Controller) Stop(ctx cell.HookContext) error {
	c.wp.Close()
	c.fastQueue.ShutDown()
	c.standardQueue.ShutDown()
	c.contextCancel()
	return nil
}

func (c *Controller) runCiliumEndpointsUpdater(ctx context.Context) error {
	for event := range c.ciliumEndpoint.Events(ctx) {
		switch event.Kind {
		case resource.Upsert:
			c.logger.WithFields(logrus.Fields{
				logfields.CEPName: event.Key.String()}).Debug("Got Upsert Endpoint event")
			c.onEndpointUpdate(event.Object)
		case resource.Delete:
			c.logger.WithFields(logrus.Fields{
				logfields.CEPName: event.Key.String()}).Debug("Got Delete Endpoint event")
			c.onEndpointDelete(event.Object)
		}
		event.Done(nil)
	}
	return nil
}

func (c *Controller) runCiliumEndpointSliceUpdater(ctx context.Context) error {
	for event := range c.ciliumEndpointSlice.Events(ctx) {
		switch event.Kind {
		case resource.Upsert:
			c.logger.WithFields(logrus.Fields{
				logfields.CESName: event.Key.String()}).Debug("Got Upsert Endpoint Slice event")
			c.onSliceUpdate(event.Object)
		case resource.Delete:
			c.logger.WithFields(logrus.Fields{
				logfields.CESName: event.Key.String()}).Debug("Got Delete Endpoint Slice event")
			c.onSliceDelete(event.Object)
		}
		event.Done(nil)
	}
	return nil
}

func (c *Controller) runCiliumNodesUpdater(ctx context.Context) error {
	ciliumNodesStore, err := c.ciliumNodes.Store(ctx)
	if err != nil {
		c.logger.WithError(err).Warn("Couldn't get CiliumNodes store")
		return err
	}
	for event := range c.ciliumNodes.Events(ctx) {
		event.Done(nil)
		totalNodes := len(ciliumNodesStore.List())
		if c.rateLimit.updateRateLimiterWithNodes(totalNodes) {
			c.logger.WithFields(logrus.Fields{
				logfields.WorkQueueQPSLimit:   c.rateLimit.current.Limit,
				logfields.WorkQueueBurstLimit: c.rateLimit.current.Burst,
			}).Info("Updated CES controller workqueue configuration")
		}
	}
	return nil
}

// Sync all CESs from cesStore to manager cache.
// Note: CESs are synced locally before CES controller running and this is required.
func (c *Controller) syncCESsInLocalCache(ctx context.Context) error {
	store, err := c.ciliumEndpointSlice.Store(ctx)
	if err != nil {
		c.logger.WithError(err).Warn("Error getting CES Store")
		return err
	}
	for _, ces := range store.List() {
		cesName := c.manager.initializeMappingForCES(ces)
		for _, cep := range ces.Endpoints {
			c.manager.initializeMappingCEPtoCES(&cep, ces.Namespace, cesName)
		}
	}
	c.logger.Debug("Successfully synced all CESs locally")
	return nil
}

// worker runs a worker thread that just dequeues items, processes them, and
// marks them done.
func (c *Controller) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) rateLimitProcessing() {
	delay := c.rateLimit.getDelay()
	select {
	case <-c.context.Done():
	case <-time.After(delay):
	}
}

func (c *Controller) getQueue() workqueue.RateLimitingInterface {
	var queue workqueue.RateLimitingInterface
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	c.logger.Infof("Standard queue length: %d, fast queue length: %d", c.standardQueue.Len(), c.fastQueue.Len())
	if c.fastQueue.Len() == 0 && c.standardQueue.Len() == 0 {
		c.logger.Infof("empty queues")
		c.cond.Wait()
		c.logger.Infof("End of waiting")
	}

	if c.fastQueue.Len() == 0 {
		queue = c.standardQueue
		c.logger.Infof("Return standard queue")
	} else {
		queue = c.fastQueue
		c.logger.Infof("Return fast queue")
	}
	return queue
}

func (c *Controller) processNextWorkItem() bool {
	c.rateLimitProcessing()
	queue := c.getQueue()
	cKey, quit := queue.Get()
	if quit {
		return false
	}
	key := cKey.(CESName)
	defer queue.Done(key)

	c.logger.WithFields(logrus.Fields{
		logfields.CESName: key.string(),
	}).Debug("Processing CES")
	defer queue.Done(key)

	queueDelay := c.getAndResetCESProcessingDelay(key)
	err := c.reconciler.reconcileCES(key)
	c.metrics.CiliumEndpointSliceQueueDelay.Observe(queueDelay)
	if err != nil {
		c.metrics.CiliumEndpointSliceSyncTotal.WithLabelValues(LabelValueOutcomeFail).Inc()
	} else {
		c.metrics.CiliumEndpointSliceSyncTotal.WithLabelValues(LabelValueOutcomeSuccess).Inc()
	}

	c.handleErr(queue, err, key)

	return true
}

func (c *Controller) handleErr(queue workqueue.RateLimitingInterface, err error, key CESName) {
	if err == nil {
		queue.Forget(key)
		return
	}

	if queue.NumRequeues(key) < maxRetries {
		time.AfterFunc(c.rateLimiter.When(key), func() {
			queue.Add(key)
			c.cond.Signal()
		})
		return
	}

	// Drop the CES from queue, we maxed out retries.
	c.logger.WithError(err).WithFields(logrus.Fields{
		logfields.CESName: key.string(),
	}).Error("Dropping the CES from queue, exceeded maxRetries")
	queue.Forget(key)
}
