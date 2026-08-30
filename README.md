# fq-client-go
GoLang client for [FQ database](https://github.com/rom8726/fq)

## Quotas

```go
changed, err := client.QuotaSet(ctx, "campaign_42", 10)
result, err := client.QuotaAcquire(ctx, "campaign_42", 4, "worker_a", 60)
released, err := client.QuotaRelease(ctx, "campaign_42", "worker_a")
```

For client-owned lease quotas, use `QuotaAcquireLease`:

```go
result, err := client.QuotaAcquireLease(ctx, "campaign_42", 10, 4, "worker_a", 60)
```
