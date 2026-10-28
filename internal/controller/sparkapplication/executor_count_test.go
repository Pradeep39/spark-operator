/*
Copyright The Kubeflow Authors.

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

package sparkapplication

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func executorPodWithPhase(appName string, id int, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-exec-%d", appName, id),
			Namespace: "default",
			Labels: map[string]string{
				common.LabelSparkAppName:    appName,
				common.LabelSparkRole:       common.SparkRoleExecutor,
				common.LabelSparkExecutorID: fmt.Sprintf("%d", id),
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func newExecutorCountReconciler(t *testing.T, maxTracked int, pods ...*corev1.Pod) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	objs := make([]client.Object, 0, len(pods))
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	return &Reconciler{
		client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		recorder: events.NewFakeRecorder(100),
		options:  Options{MaxTrackedExecutorPerApp: maxTracked},
	}
}

func runningApp(appName string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: appName, Namespace: "default"},
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{State: v1beta2.ApplicationStateRunning},
		},
	}
}

func TestUpdateExecutorStateCountsActiveExecutors(t *testing.T) {
	r := newExecutorCountReconciler(t, 1000,
		executorPodWithPhase("app", 1, corev1.PodRunning),
		executorPodWithPhase("app", 2, corev1.PodPending), // e.g. held by a scheduling gate
		executorPodWithPhase("app", 3, corev1.PodSucceeded),
		executorPodWithPhase("app", 4, corev1.PodFailed),
		executorPodWithPhase("other", 9, corev1.PodRunning),
	)
	app := runningApp("app")

	require.NoError(t, r.updateExecutorState(context.Background(), app))

	assert.Equal(t, ptr.To[int32](2), app.Status.ActiveExecutors)
	assert.Equal(t, ptr.To[int64](4), app.Status.MaxExecutorID)
	assert.Len(t, app.Status.ExecutorState, 4)
}

func TestUpdateExecutorStateCountsBeyondMaxTrackedExecutorPerApp(t *testing.T) {
	r := newExecutorCountReconciler(t, 1,
		executorPodWithPhase("app", 1, corev1.PodRunning),
		executorPodWithPhase("app", 2, corev1.PodRunning),
		executorPodWithPhase("app", 3, corev1.PodPending),
	)
	app := runningApp("app")

	require.NoError(t, r.updateExecutorState(context.Background(), app))

	assert.Len(t, app.Status.ExecutorState, 1)
	assert.Equal(t, ptr.To[int32](3), app.Status.ActiveExecutors)
	assert.Equal(t, ptr.To[int64](3), app.Status.MaxExecutorID)
}

func TestUpdateExecutorStateLeavesBothUnsetWithoutExecutors(t *testing.T) {
	r := newExecutorCountReconciler(t, 1000)
	app := runningApp("app")

	require.NoError(t, r.updateExecutorState(context.Background(), app))

	assert.Nil(t, app.Status.ActiveExecutors)
	assert.Nil(t, app.Status.MaxExecutorID)
}

func TestUpdateExecutorStateKeepsTheHighestExecutorIDWhenExecutorsGo(t *testing.T) {
	r := newExecutorCountReconciler(t, 1000, executorPodWithPhase("app", 2, corev1.PodRunning))
	app := runningApp("app")
	app.Status.ActiveExecutors = ptr.To[int32](5)
	app.Status.MaxExecutorID = ptr.To[int64](7)

	require.NoError(t, r.updateExecutorState(context.Background(), app))
	assert.Equal(t, ptr.To[int32](1), app.Status.ActiveExecutors)
	assert.Equal(t, ptr.To[int64](7), app.Status.MaxExecutorID)

	// Once every executor is gone, the count drops to zero rather than keeping its last value
	r = newExecutorCountReconciler(t, 1000)
	require.NoError(t, r.updateExecutorState(context.Background(), app))
	assert.Equal(t, ptr.To[int32](0), app.Status.ActiveExecutors)
	assert.Equal(t, ptr.To[int64](7), app.Status.MaxExecutorID)
}

func TestResetSparkApplicationStatusClearsActiveExecutorsAndMaxExecutorID(t *testing.T) {
	for _, state := range []v1beta2.ApplicationStateType{
		v1beta2.ApplicationStateSucceeding,
		v1beta2.ApplicationStateFailing,
		v1beta2.ApplicationStateInvalidating,
		v1beta2.ApplicationStateSuspended,
	} {
		t.Run(string(state), func(t *testing.T) {
			app := runningApp("app")
			app.Status.AppState.State = state
			app.Status.ActiveExecutors = ptr.To[int32](3)
			app.Status.MaxExecutorID = ptr.To[int64](9)

			(&Reconciler{}).resetSparkApplicationStatus(app)

			assert.Nil(t, app.Status.ActiveExecutors)
			assert.Nil(t, app.Status.MaxExecutorID)
		})
	}
}
