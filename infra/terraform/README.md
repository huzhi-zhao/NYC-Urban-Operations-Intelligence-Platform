# Terraform Setup for NYC-UOIP

## Prerequisites

1. Install Terraform >= 1.5.0
2. Install Google Cloud SDK (`gcloud`) `brew install --cask google-cloud-sdk`  
3. Authenticate with GCP: `gcloud auth login`
    - `gcloud auth application-default login` 生成json凭证  Credentials saved to file: （/Users/jimmy/.config/gcloud/application_default_credentials.json）
4. Set your project: `gcloud config set project $YOUR_PROJECT_ID`

## Quick Start

### 1. Configure variables

```bash
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project_id and resource names
```

### 2. Initialize Terraform

```bash
cd infra/terraform
terraform init
```

### 3. Plan and apply

```bash
# Preview changes
terraform plan

# Apply changes (creates resources and saves key locally)
terraform apply
```

### 4. Configure local environment

After `terraform apply`, the service account key will be saved at:
```
infra/terraform/keys/nyc-uoip-sa-key.json
```

Set the environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/nyc-uoip/infra/terraform/keys/nyc-uoip-sa-key.json"
```

Add to your `.env` file:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/nyc-uoip/infra/terraform/keys/nyc-uoip-sa-key.json
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=nyc-uoip
BIGQUERY_DATASET=nyc_uoip
```

## What Gets Created

| Resource | Description |
|----------|-------------|
| `google_service_account.main` | Service account for NYC-UOIP |
| `google_service_account_key.main` | RSA 2048 key saved to `keys/` directory |
| `google_project_iam_member.storage_object_admin` | GCS read/write permissions |
| `google_project_iam_member.bigquery_data_editor` | BigQuery data write permissions |
| `google_project_iam_member.bigquery_job_user` | BigQuery job execution permissions |
| `google_project_iam_member.dataproc_editor` | Dataproc job permissions |
| `google_storage_bucket.bronze` | GCS bucket for Bronze layer (versioned) |
| `google_bigquery_dataset.main` | BigQuery dataset for Gold layer |

## Troubleshooting

### Error: "Key creation is not allowed on this service account"

**原因**：GCP 组织级别策略 `constraints/iam.disableServiceAccountKeyCreation` 被设置为 `DENY`，阻止创建 Service Account Key。

**解决方案**：

1. **方案 A：修改组织策略（推荐）**

   在 GCP Console 中操作：
   - IAM → Organization Policies
   - 搜索 `disableServiceAccountKeyCreation`
   - 编辑为 `Allow`

   或使用 gcloud CLI：
   ```shell
   # 查看当前项目所有组织策略
   gcloud org-policies list --project=pace-lab-bdp

   # 允许 SA key 创建（项目级别）
    printf 'name: projects/pace-lab-bdp/policies/constraints/iam.disableServiceAccountKeyCreation
     spec:
       listPolicy:
         allow:
           values:
             - projects/pace-lab-bdp
     ' > /tmp/allow-sa-key-policy.yaml

   gcloud org-policies set-policy /tmp/allow-sa-key-policy.yaml --project=pace-lab-bdp
   ```

   等待几分钟后重试：
   ```bash
   gcloud iam service-accounts keys create keys/nyc-uoip-sa-key.json \
     --iam-account=nyc-uoip-sa@pace-lab-bdp.iam.gserviceaccount.com
   ```


## Security Notes

- **NEVER** commit the key file to git. It's excluded via `.gitignore`
- The key file is saved locally so you can use it for local development
- For production (GCP Cloud Run/Composer), use Workload Identity Federation instead
- Rotate keys periodically: `terraform apply -replace=google_service_account_key.main`

## Destroy Resources

```bash
terraform destroy
```