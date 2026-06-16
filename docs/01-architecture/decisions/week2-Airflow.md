## Backfill by Airflow

![airflow-architexture-overview](../../images/Airflow-Backfill_Architecture-Overview.png)

### GCP Composer 启动
```shell
# 相当于 `cd infra/terraform && terraform apply`
make terraform-apply

# 3. 等创建完成后，部署代码
make deploy-composer

# 4. 浏览器打开 Airflow UI
terraform -chdir=infra/terraform output composer_airflow_uri
```

### VM端Docker部署

```shell
cd infra/docker
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```
