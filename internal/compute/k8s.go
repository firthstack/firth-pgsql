package compute

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Runtime starts/stops compute instances for endpoints. The control plane is
// the only writer; pods are cattle — all durable state lives in the storage
// stack.
type Runtime interface {
	Start(ctx context.Context, endpointID string, cfg ComputeConfig) error
	Stop(ctx context.Context, endpointID string) error
	Status(ctx context.Context, endpointID string) (PodStatus, error)
}

type PodStatus struct {
	Exists bool
	PodIP  string
	Phase  string
}

type K8sRuntime struct {
	client       kubernetes.Interface
	namespace    string
	computeImage string
}

func NewK8sRuntime(client kubernetes.Interface, namespace, computeImage string) *K8sRuntime {
	return &K8sRuntime{client: client, namespace: namespace, computeImage: computeImage}
}

func podName(endpointID string) string       { return "compute-" + endpointID }
func configMapName(endpointID string) string { return "compute-" + endpointID + "-config" }

func (r *K8sRuntime) Start(ctx context.Context, endpointID string, cfg ComputeConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(endpointID),
			Namespace: r.namespace,
			Labels:    map[string]string{"app": "compute", "endpoint": endpointID},
		},
		Data: map[string]string{"config.json": string(raw)},
	}
	if _, err := r.client.CoreV1().ConfigMaps(r.namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create configmap: %w", err)
		}
		// Refresh spec for a retried start.
		if _, err := r.client.CoreV1().ConfigMaps(r.namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update configmap: %w", err)
		}
	}
	if _, err := r.client.CoreV1().Pods(r.namespace).Create(ctx, r.buildPod(endpointID), metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create pod: %w", err)
		}
	}
	return nil
}

func (r *K8sRuntime) buildPod(endpointID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(endpointID),
			Namespace: r.namespace,
			Labels:    map[string]string{"app": "compute", "endpoint": endpointID},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            "compute",
				Image:           r.computeImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{
					"/usr/local/bin/compute_ctl",
					"--pgdata", "/var/db/postgres/compute",
					"-C", "postgresql://cloud_admin@localhost:55433/postgres",
					"-b", "/usr/local/bin/postgres",
					"--compute-id", podName(endpointID),
					"--config", "/config/config.json",
					"--dev",
				},
				Ports: []corev1.ContainerPort{
					{ContainerPort: 55433, Name: "pg"},
					{ContainerPort: 3080, Name: "http"},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("250m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "config", MountPath: "/config"},
					{Name: "pgdata", MountPath: "/var/db/postgres/compute"},
				},
			}},
			Volumes: []corev1.Volume{
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(endpointID)},
						},
					},
				},
				{
					Name:         "pgdata",
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				},
			},
		},
	}
}

func (r *K8sRuntime) Stop(ctx context.Context, endpointID string) error {
	if err := r.client.CoreV1().Pods(r.namespace).Delete(ctx, podName(endpointID), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod: %w", err)
	}
	if err := r.client.CoreV1().ConfigMaps(r.namespace).Delete(ctx, configMapName(endpointID), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete configmap: %w", err)
	}
	return nil
}

func (r *K8sRuntime) Status(ctx context.Context, endpointID string) (PodStatus, error) {
	pod, err := r.client.CoreV1().Pods(r.namespace).Get(ctx, podName(endpointID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return PodStatus{Exists: false}, nil
	}
	if err != nil {
		return PodStatus{}, err
	}
	return PodStatus{Exists: true, PodIP: pod.Status.PodIP, Phase: string(pod.Status.Phase)}, nil
}
