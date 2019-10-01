/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rancher

import (
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	klog "k8s.io/klog/v2"
)

const (
	// GPULabel is the label added to nodes with GPU resource.
	GPULabel = "nodes.pkds.it/gpu-node"
)

// CloudProvider contains configuration info and functions for interacting with
// cloud provider (GCE, AWS, etc).
type rancherCloudProvider struct {
	manager         *RancherManager
	resourceLimiter *cloudprovider.ResourceLimiter
}

// BuildRancher builds Rancher cloud provider, manager etc.
func BuildRancher(opts config.AutoscalingOptions, do cloudprovider.NodeGroupDiscoveryOptions, rl *cloudprovider.ResourceLimiter) cloudprovider.CloudProvider {
	manager, err := BuildRancherManager()
	if err != nil {
		klog.Fatalf("Failed to create Rancher Manager: %v", err)
	}
	provider, err := BuildRancherCloudProvider(manager, rl)
	if err != nil {
		klog.Fatalf("Failed to create Rancher cloud provider: %v", err)
	}
	return provider
}

// BuildRancherCloudProvider creates new rancherCloudProvider
func BuildRancherCloudProvider(manager *RancherManager, resourceLimiter *cloudprovider.ResourceLimiter) (cloudprovider.CloudProvider, error) {
	rancher := &rancherCloudProvider{
		manager:         manager,
		resourceLimiter: resourceLimiter,
	}

	return rancher, nil
}

// Name returns name of the cloud provider.
func (cp *rancherCloudProvider) Name() string {
	return cloudprovider.RancherProviderName
}

// NodeGroups returns all node groups configured for this cloud provider.
func (cp *rancherCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	nps, err := cp.manager.GetCachedNodePools()
	if err != nil {
		klog.Errorf("failed to get node pools: %s", err)
		return nil
	}

	return nps
}

// NodeGroupForNode returns the node group for the given node, nil if the node
// should not be processed by cluster autoscaler, or non-nil error if such
// occurred. Must be implemented.
func (cp *rancherCloudProvider) NodeGroupForNode(node *v1.Node) (cloudprovider.NodeGroup, error) {
	rancherNode, err := cp.manager.GetCachedNodeForKubernetesNode(node.Name)
	if err != nil {
		return nil, err
	}

	if rancherNode.NodePoolID == "" {
		return nil, fmt.Errorf("missing node pool name for node %s (%s)", rancherNode.NodeName, rancherNode.ID)
	}

	return &rancherNodeGroup{
		manager: cp.manager,
		id:      rancherNode.NodePoolID,
	}, nil
}

// Pricing returns pricing model for this cloud provider or error if not available.
// Implementation optional.
func (cp *rancherCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (cp *rancherCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
// Implementation optional.
func (cp *rancherCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []v1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (cp *rancherCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return cp.resourceLimiter, nil
}

// GPULabel returns the label added to nodes with GPU resource.
func (cp *rancherCloudProvider) GPULabel() string {
	return GPULabel
}

// GetAvailableGPUTypes return all available GPU types cloud provider supports.
func (cp *rancherCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	return nil
}

// Cleanup cleans up open resources before the cloud provider is destroyed, i.e. go routines etc.
func (cp *rancherCloudProvider) Cleanup() error {
	return cp.manager.Cleanup()
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (cp *rancherCloudProvider) Refresh() error {
	return cp.manager.Refresh()
}
