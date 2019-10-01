package rancher

import (
	"fmt"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

const (
	// ProviderName is the cloud provider name for AWS
	ProviderName = "rancher"
)

type rancherCloudProvider struct {
	manager			*RancherManager
	resourceLimiter	*cloudprovider.ResourceLimiter
}

// CloudProvider contains configuration info and functions for interacting with
// cloud provider (GCE, AWS, etc).
func BuildRancherCloudProvider(manager *RancherManager, resourceLimiter *cloudprovider.ResourceLimiter) (cloudprovider.CloudProvider, error) {
	rancher := &rancherCloudProvider{
		manager:         manager,
		resourceLimiter: resourceLimiter,
	}

	return rancher, nil
}

// Name returns name of the cloud provider.
func (cp *rancherCloudProvider) Name() string {
	return ProviderName
}

// NodeGroups returns all node groups configured for this cloud provider.
func (cp *rancherCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	nps, err := cp.manager.GetCachedNodePools()
	if err != nil {
		glog.Errorf("failed to get node pools: %s", err)
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
		id:	rancherNode.NodePoolID,
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

// GetInstanceID gets the instance ID for the specified node.
func (cp *rancherCloudProvider) GetInstanceID(node *v1.Node) string {
	rancherNode, err := cp.manager.GetCachedNodeForKubernetesNode(node.Name)
	if err != nil {
		glog.Errorf("failed to find node with name %s: %s", node.Name, err)
		return node.Name
	}

	return rancherNode.ID
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
