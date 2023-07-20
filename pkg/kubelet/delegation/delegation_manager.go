package delegation

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mdlayher/vsock"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/status/state"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// podStatusManagerStateFile is the file name where status manager stores its state
const podStatusManagerStateFile = "pod_status_manager_state"
const podCleanupTimeout = 30 * time.Second
const podCleanupPollFreq = time.Second

// KillPodFunc kills a pod.
// The pod status is updated, and then it is killed with the specified grace period.
// This function must block until either the pod is killed or an error is encountered.
// Arguments:
// pod - the pod to kill
// status - the desired status to associate with the pod (i.e. why its killed)
// gracePeriodOverride - the grace period override to use instead of what is on the pod spec
type KillPodFunc func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error

// PodCleanedUpFunc returns true if all resources associated with a pod have been reclaimed.
type PodCleanedUpFunc func(*v1.Pod) bool

// Updates pod specs in apiserver for node assignment.
// Communicate with CRI shim on host.
// Patch pod info to kubeGenericRuntimeManager.
// All methods are thread-safe.
type manager struct {
	//  used to track time
	clock       clock.WithTicker
	killPodFunc KillPodFunc
	kubeClient  clientset.Interface
	podManager  PodManager
	// Map from pod UID to configs of the corresponding pod.
	podConfigs map[types.UID]runtimeapi.PodSandboxConfig
	// Map from pod UID to container name to container configs.
	containerConfigs map[types.UID]map[string]runtimeapi.ContainerConfig
	podConfigsLock   sync.RWMutex
	podConfigChannel chan struct{}
	// state allows to save/restore pod resource allocation and tolerate kubelet restarts.
	state state.State
	// stateFileDirectory holds the directory where the state file for checkpoints is held.
	stateFileDirectory string
	// userName to identify the delegated kubelet
	userName string
	// nodeName to identify node
	nodeName string
}

// PodManager is the subset of methods the manager needs to observe the actual state of the kubelet.
// See pkg/k8s.io/kubernetes/pkg/kubelet/pod.Manager for method godoc.
type PodManager interface {
	GetPodByUID(types.UID) (*v1.Pod, bool)
	GetMirrorPodByPod(*v1.Pod) (*v1.Pod, bool)
	TranslatePodUID(uid types.UID) kubetypes.ResolvedPodUID
	GetUIDTranslations() (podToMirror map[kubetypes.ResolvedPodUID]kubetypes.MirrorPodUID, mirrorToPod map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID)
}

type Manager interface {
	// Start the VM sockets server.
	Start(podCleanedUpFunc PodCleanedUpFunc)
	// Get container config
	GetContainerConfig(uid types.UID, contName string) runtimeapi.ContainerConfig
}

const maxByte = 4096
const syncPeriod = 10 * time.Second
const vsockHostPort = 1024

// NewManager returns a functional Manager.
func NewManager(clock clock.WithTicker, killPodFunc KillPodFunc, kubeClient clientset.Interface, podManager PodManager, stateFileDirectory string, userName string, nodeName string) Manager {
	return &manager{
		clock:              clock,
		killPodFunc:        killPodFunc,
		kubeClient:         kubeClient,
		podManager:         podManager,
		podConfigs:         make(map[types.UID]runtimeapi.PodSandboxConfig),
		containerConfigs:   make(map[types.UID]map[string]runtimeapi.ContainerConfig),
		podConfigChannel:   make(chan struct{}, 1),
		stateFileDirectory: stateFileDirectory,
		userName:           userName,
		nodeName:           nodeName,
	}
}

func (m *manager) Start(podCleanedUpFunc PodCleanedUpFunc) {
	// Initialize m.state to no-op state checkpoint manager
	m.state = state.NewNoopStateCheckpoint()

	// Create pod allocation checkpoint manager even if client is nil so as to allow local get/set of AllocatedResources & Resize
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		stateImpl, err := state.NewStateCheckpoint(m.stateFileDirectory, podStatusManagerStateFile)
		if err != nil {
			// This is a crictical, non-recoverable failure.
			klog.ErrorS(err, "Could not initialize pod allocation checkpoint manager, please drain node and remove policy state file")
			panic(err)
		}
		m.state = stateImpl
	}

	// Don't start the delegation manager if we don't have a client. This will happen
	// on the master, where the kubelet is responsible for bootstrapping the pods
	// of the master components.
	if m.kubeClient == nil {
		klog.InfoS("Kubernetes client is nil, not starting status manager")
		return
	}

	// Launch a VM sockets server to receive the cri requests
	go wait.Forever(func() {
		addr, err := m.connectHost()
		port, err := strconv.ParseUint(strings.Split(addr.String(), ":")[1], 10, 32)
		if err != nil {
			fmt.Printf("Failed to parse the port from  %s: %s\n", addr.String(), err)
			return
		}

		l, err := vsock.Listen(uint32(port), nil)
		if err != nil {
			panic(err)
		}
		defer l.Close()
		for {
			// Accept a single connection.
			c, err := l.Accept()
			if err != nil {
				panic(err)
			}
			defer c.Close()
			// Process the request.
			// TODO: how to avoid setting maxByte?
			b := make([]byte, maxByte)
			n, err := c.Read(b)
			if err != nil {
				panic(err)
			}
			// Assume on the host side, it encodes the request type and request
			// by len(request_type) || request_type || request
			// len(request_type) takes only one byte
			typeLen := int(b[0])
			typeName := string(b[1 : 1+typeLen])
			// Assume the request is serialized using json
			// So now we deserialize it
			switch typeName {
			case "RunPodSandbox":
				req := &runtimeapi.RunPodSandboxRequest{}
				err = json.Unmarshal(b[1+typeLen:n], req)
				if err != nil {
					panic(err)
				}
				// pod.UID is determined at API server https://github.com/kubernetes/apiserver/blob/0e613811b6d0e41341abffac5a2f423eeee0fbaf/pkg/registry/rest/meta.go#L36
				// Assign node to pod, thus the kubelet can fetch the pod using the original mechanism
				// Ref to the codes of scheduler. There is a blog for it: https://zhuanlan.zhihu.com/p/344909204
				config := req.Config
				binding := &v1.Binding{
					ObjectMeta: metav1.ObjectMeta{Namespace: config.Metadata.Namespace, Name: config.Metadata.Name, UID: types.UID(config.Metadata.Uid)},
					Target:     v1.ObjectReference{Kind: "Node", Name: m.nodeName},
				}
				m.kubeClient.CoreV1().Pods(binding.Namespace).Bind(context.TODO(), binding, metav1.CreateOptions{})
				// Insert (UID, config) into the map
				m.podConfigs[types.UID(config.Metadata.Uid)] = *config
				// TODO: may need to send back the status. Currently, it seems not necessary.
			case "CreateContainer":
				req := &runtimeapi.CreateContainerRequest{}
				err = json.Unmarshal(b[1+typeLen:n], req)
				if err != nil {
					panic(err)
				}
				contConfig := req.Config
				contName := contConfig.Metadata.Name
				podUid := req.SandboxConfig.Metadata.Uid
				contConfigMap, found := m.containerConfigs[types.UID(podUid)]
				if !found {
					contConfigMap = make(map[string]runtimeapi.ContainerConfig)
				}
				contConfigMap[contName] = *contConfig

			case "StopPodSandbox":
				// currently, only eviction/preemption goes this branch
				req := &runtimeapi.StopPodSandboxRequest{}
				err = json.Unmarshal(b[1+typeLen:n], req)
				if err != nil {
					panic(err)
				}
				// assume host CRI shim assigns the pod UID to PodSandboxId for RunPodSandboxResponse
				// so when the host kubelet wants to kill the pod, it uses PodSandboxId
				// in this case, it is pod UID
				podUid := req.PodSandboxId
				podConfig := m.podConfigs[types.UID(podUid)]
				pod, err := m.kubeClient.CoreV1().Pods(podConfig.Metadata.Namespace).Get(context.TODO(), podConfig.Metadata.Name, metav1.GetOptions{})
				// if pod object does not exist in etcd or pod is being delated
				// it is not a eviction case
				// Currently it needs a round of network traffic. It may be further optimized.
				if err == nil && pod.DeletionGracePeriodSeconds == nil {
					go func() {
						m.evictPod(pod, 0, "eviction from host", nil, nil)
						m.waitForPodsCleanup(podCleanedUpFunc, []*v1.Pod{pod})
					}()
				}

			default:
				panic("no corresponding operation")
			}
		}
	}, 0)
}

func (m *manager) GetContainerConfig(uid types.UID, contName string) runtimeapi.ContainerConfig {
	//TODO: may need lock
	contConfigMap, found := m.containerConfigs[uid]
	if !found {
		klog.V(4).InfoS("ContainerConfig has been deleted", "podUID", string(uid))
		panic("ContainerConfig has been deleted")
	}
	config, found := contConfigMap[contName]
	if !found {
		klog.V(4).InfoS("ContainerConfig has been deleted", "container name", contName)
		panic("ContainerConfig has been deleted")
	}
	return config
}

func (m *manager) connectHost() (net.Addr, error) {
	// Dial a VM sockets connection to a process on the hypervisor
	// Ref: https://mdlayher.com/blog/linux-vm-sockets-in-go/
	// bound to port vsockHostPort=1024.
	c, err := vsock.Dial(vsock.Host, vsockHostPort, nil)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// Send message username to the hypervisor for building the map.
	// No need to send ContextID and Port, because at the host it can directly call RemoteAddr on the connection
	if _, err := c.Write([]byte(m.userName)); err != nil {
		return nil, err
	}

	// Read back the ok response from the hypervisor, indicating that the addr is persisted.
	// TODO: if err occurs, periodically reconnect the hypervisor to do the same thing
	b := make([]byte, 16)
	n, err := c.Read(b)
	if err != nil || "ok" != string(b[:n]) {
		return nil, err
	}

	return c.LocalAddr(), nil
}

// Similar to the evictPod method in evictionManager
func (m *manager) evictPod(pod *v1.Pod, gracePeriodOverride int64, evictMsg string, annotations map[string]string, condition *v1.PodCondition) bool {
	// this is a blocking call and should only return when the pod and its containers are killed.
	klog.V(3).InfoS("Evicting pod", "pod", klog.KObj(pod), "podUID", pod.UID, "message", evictMsg)
	err := m.killPodFunc(pod, true, &gracePeriodOverride, func(status *v1.PodStatus) {
		status.Phase = v1.PodFailed
		status.Reason = "Evicted"
		status.Message = evictMsg
		if condition != nil {
			podutil.UpdatePodCondition(status, condition)
		}
	})
	if err != nil {
		klog.ErrorS(err, "Eviction manager: pod failed to evict", "pod", klog.KObj(pod))
	} else {
		klog.InfoS("Eviction manager: pod is evicted successfully", "pod", klog.KObj(pod))
	}
	return true
}

// Similar to the waitForPodsCleanup method in evictionManager
func (m *manager) waitForPodsCleanup(podCleanedUpFunc PodCleanedUpFunc, pods []*v1.Pod) {
	timeout := m.clock.NewTimer(podCleanupTimeout)
	defer timeout.Stop()
	ticker := m.clock.NewTicker(podCleanupPollFreq)
	defer ticker.Stop()
	for {
		select {
		case <-timeout.C():
			klog.InfoS("Eviction manager: timed out waiting for pods to be cleaned up", "pods", klog.KObjSlice(pods))
			return
		case <-ticker.C():
			for i, pod := range pods {
				if !podCleanedUpFunc(pod) {
					break
				}
				if i == len(pods)-1 {
					klog.InfoS("Eviction manager: pods successfully cleaned up", "pods", klog.KObjSlice(pods))
					return
				}
			}
		}
	}
}
