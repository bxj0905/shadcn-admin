# Prefect Job 和 Pod 清理 CronJob

这个目录包含用于自动清理 Prefect 已完成的 Job 和 Pod 的 Kubernetes CronJob 配置。

## 文件说明

- `prefect-cleanup-cronjob.yaml`: CronJob 定义，每天 0 点执行清理任务
- `prefect-cleanup-rbac.yaml`: ServiceAccount 和 RBAC 权限配置

## 部署步骤

### 1. 创建 RBAC 资源

```bash
kubectl apply -f k8s/prefect-cleanup-rbac.yaml
```

### 2. 创建 CronJob

```bash
kubectl apply -f k8s/prefect-cleanup-cronjob.yaml
```

## 配置说明

### 执行时间

CronJob 默认在每天 UTC 0 点执行。如果需要调整时区：

- **北京时间 0 点**: 修改 `schedule` 为 `"0 0 16 * * *"` (UTC+8)
- **其他时区**: 根据 UTC 偏移量调整

### 清理内容

CronJob 会清理以下资源：

1. **已完成的 Job**: 状态为 `succeeded == 1` 的 Job
2. **已完成的 Pod**: 状态为 `Succeeded` 的 Pod
3. **失败的 Pod**: 状态为 `Failed` 的 Pod（可选）
4. **孤立的 Pod**: 没有关联 Job 且已完成或失败的 Pod

### 保留策略

- `successfulJobsHistoryLimit: 3`: 保留最近 3 次成功的执行记录
- `failedJobsHistoryLimit: 1`: 保留最近 1 次失败的执行记录
- `ttlSecondsAfterFinished: 3600`: CronJob 创建的 Job 完成后 1 小时自动删除

## 验证

### 查看 CronJob 状态

```bash
kubectl get cronjob -n prefect prefect-job-cleanup
```

### 查看执行历史

```bash
kubectl get jobs -n prefect -l app=prefect-cleanup
```

### 查看最近一次执行的日志

```bash
# 获取最近的 Job 名称
JOB_NAME=$(kubectl get jobs -n prefect -l app=prefect-cleanup --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')

# 查看 Pod 日志
kubectl logs -n prefect -l job-name=$JOB_NAME
```

### 手动触发执行（测试）

```bash
kubectl create job --from=cronjob/prefect-job-cleanup -n prefect prefect-job-cleanup-manual-$(date +%s)
```

## 注意事项

1. **权限**: CronJob 使用的 ServiceAccount 只有删除 Job 和 Pod 的权限，不会影响其他资源
2. **时区**: 确保 CronJob 的执行时间符合你的时区要求
3. **依赖**: 需要集群中安装 `jq` 工具（bitnami/kubectl 镜像已包含）
4. **命名空间**: 确保 `prefect` 命名空间存在

## 故障排查

如果 CronJob 没有执行：

1. 检查 CronJob 状态：
   ```bash
   kubectl describe cronjob -n prefect prefect-job-cleanup
   ```

2. 检查 ServiceAccount 和权限：
   ```bash
   kubectl get serviceaccount -n prefect prefect-cleanup
   kubectl get rolebinding -n prefect prefect-cleanup-rolebinding
   ```

3. 查看最近的 Job 日志：
   ```bash
   kubectl logs -n prefect -l app=prefect-cleanup --tail=100
   ```

