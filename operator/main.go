package main

import (
	"context"
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	intstrpkg "k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var groupVersion = schema.GroupVersion{Group: "bog.example.com", Version: "v1alpha1"}

type BogSpec struct {
	Image            string  `json:"image"`
	Replicas         int32   `json:"replicas"`
	Storage          string  `json:"storage"`
	StorageClassName *string `json:"storageClassName,omitempty"`
}
type Bog struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              BogSpec `json:"spec"`
}
type BogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bog `json:"items"`
}

func (b *Bog) DeepCopyObject() runtime.Object {
	out := new(Bog)
	*out = *b
	out.ObjectMeta = *b.ObjectMeta.DeepCopy()
	if b.Spec.StorageClassName != nil {
		x := *b.Spec.StorageClassName
		out.Spec.StorageClassName = &x
	}
	return out
}
func (b *BogList) DeepCopyObject() runtime.Object {
	out := new(BogList)
	*out = *b
	out.ListMeta = b.ListMeta
	out.Items = make([]Bog, len(b.Items))
	for i := range b.Items {
		out.Items[i] = *(b.Items[i].DeepCopyObject().(*Bog))
	}
	return out
}

type reconciler struct {
	client.Client
	scheme *runtime.Scheme
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var b Bog
	if err := r.Get(ctx, req.NamespacedName, &b); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if b.Spec.Replicas != 1 {
		return ctrl.Result{}, fmt.Errorf("replicas must be 1: Bog does not yet implement replication or quorum")
	}
	if b.Spec.Image == "" {
		return ctrl.Result{}, fmt.Errorf("image is required")
	}
	size, err := resource.ParseQuantity(b.Spec.Storage)
	if err != nil || size.Sign() <= 0 {
		return ctrl.Result{}, fmt.Errorf("storage must be a positive quantity")
	}
	labels := map[string]string{"app.kubernetes.io/name": "bog", "app.kubernetes.io/instance": b.Name}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: b.Name, Namespace: b.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(&b, svc, r.scheme); err != nil {
			return err
		}
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 8080}}
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: b.Name, Namespace: b.Namespace}, sts)
	if err == nil {
		if !metav1.IsControlledBy(sts, &b) {
			return ctrl.Result{}, fmt.Errorf("StatefulSet is not owned by Bog")
		}
		if len(sts.Spec.Template.Spec.Containers) != 1 || sts.Spec.Template.Spec.Containers[0].Image != b.Spec.Image {
			return ctrl.Result{}, fmt.Errorf("image change blocked: client, peer and WAL compatibility cannot be verified")
		}
		if *sts.Spec.Replicas != 1 {
			return ctrl.Result{}, fmt.Errorf("unexpected replica count; manual intervention required")
		}
		return ctrl.Result{}, nil
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	one := int32(1)
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr(8080)}}, PeriodSeconds: 10, FailureThreshold: 360}
	sts = &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: b.Name, Namespace: b.Namespace}, Spec: appsv1.StatefulSetSpec{ServiceName: b.Name, Replicas: &one, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/os": "linux"}, Containers: []corev1.Container{{Name: "bog", Image: b.Spec.Image, Args: []string{"-data-dir=/data", "-addr=0.0.0.0:8080"}, Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}, StartupProbe: probe, ReadinessProbe: &corev1.Probe{ProbeHandler: probe.ProbeHandler, PeriodSeconds: 5}, VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}}}}}, VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: b.Spec.StorageClassName, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}}}}}}}
	if err = controllerutil.SetControllerReference(&b, sts, r.scheme); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.Create(ctx, sts)
}
func intstr(n int) intstrpkg.IntOrString { return intstrpkg.FromInt32(int32(n)) }
func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	s.AddKnownTypes(groupVersion, &Bog{}, &BogList{})
	metav1.AddToGroupVersion(s, groupVersion)
	m, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: s})
	if err == nil {
		err = ctrl.NewControllerManagedBy(m).For(&Bog{}).Owns(&appsv1.StatefulSet{}).Owns(&corev1.Service{}).Complete(&reconciler{m.GetClient(), s})
	}
	if err == nil {
		err = m.Start(ctrl.SetupSignalHandler())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
