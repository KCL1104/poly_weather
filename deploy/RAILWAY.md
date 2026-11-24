# 部署到 Railway

Railway 沒有 systemd，一個服務只能掛一顆磁碟，排程服務也不能共用磁碟。所以在 Railway 上整套系統跑在**一個服務、一個容器**裡：

```
polyweather run-all
├─ record            訂單簿紀錄器（常駐，掛掉自動重啟）
├─ metar poll        METAR 輪詢（常駐）
└─ 定時工作          catalog／nbm／lamp 每 10 分鐘、ensemble 每小時、
                     offload 每 15 分鐘、prices 與兩項檢查每天
```

repo 根目錄的 `Dockerfile` 會被 Railway 自動採用，啟動指令就是 `polyweather run-all`。空資料目錄第一次啟動時，會先做匯入模式的盤口掃描，並回補 4 天 METAR。

## 儲存

| 放哪裡 | 放什麼 | 大小 | 單價 |
| --- | --- | --- | --- |
| 磁碟（Volume，掛在 `/data`） | SQLite 資料庫、最近 12 小時的原始檔 | 5 GB 就夠 | 每 GB 每月 0.15 美元 |
| Bucket（S3 相容） | 全部原始訂單簿、快照、集成預報、價格序列、每日資料庫備份 | 兩週約 35 到 75 GB | 每 GB 每月 0.015 美元，下載免費 |

- 每個 Parquet 檔寫完就不再變動。`offload` 每 15 分鐘把新檔上傳，記在 `offloaded` 表，超過保留時數的本機副本才刪除。
- 磁碟用量超過 85% 會發告警。Bucket 連不上時本機檔案不會被刪，磁碟大約一天會滿。
- Hobby 方案的磁碟上限是 5 GB，Bucket 合計上限 1 TB；Pro 的磁碟可自助擴到 1 TB，Bucket 不限。

## 服務設定

| 設定 | 值 | 原因 |
| --- | --- | --- |
| 來源 | GitHub `KCL1104/poly_weather`，指定分支 | |
| 磁碟 | 掛載路徑 `/data`，5 GB | 程式會自動用掛載的磁碟當資料目錄 |
| 重啟策略 | `ALWAYS` | |
| Draining | 30 秒 | 重新部署時讓紀錄器把緩衝寫完 |
| 副本數 | 1 | 有磁碟的服務只能單一副本 |
| Watch patterns | `src/**`、`pyproject.toml`、`Dockerfile` | 只改文件或研究腳本時不重新部署 |
| 健康檢查 | 不設 | 服務不對外開埠 |

每次重新部署，紀錄器會中斷約半分鐘到一分鐘，這段會記成 gap。

## 變數

| 變數 | 值 |
| --- | --- |
| `POLYWEATHER_SCOPE` | `us` |
| `POLYWEATHER_LOCAL_RETENTION_HOURS` | `12`（5 GB 磁碟）；磁碟大就調高 |
| `AWS_ENDPOINT_URL` | `${{<bucket>.ENDPOINT}}` |
| `AWS_S3_BUCKET_NAME` | `${{<bucket>.BUCKET}}` |
| `AWS_DEFAULT_REGION` | `${{<bucket>.REGION}}` |
| `AWS_ACCESS_KEY_ID` | `${{<bucket>.ACCESS_KEY_ID}}` |
| `AWS_SECRET_ACCESS_KEY` | `${{<bucket>.SECRET_ACCESS_KEY}}` |
| `POLYWEATHER_S3_ADDRESSING_STYLE` | 不用設。程式會先試 `virtual`（Railway 現在的 Bucket）、再試 `path`（舊 Bucket），記住能用的那個。要強制時才填 `virtual` 或 `path` |
| `ALERT_WEBHOOK_URL` | 選填，Slack 或 Discord 的 incoming webhook |

Bucket 的五個值用變數參照，不要複製貼上：金鑰不會出現在任何人的畫面上，重設金鑰後也不必改。

## 花費估計

以 2026-11-24 的定價和實測用量估，每月大約 15 到 25 美元：

| 項目 | 用量 | 每月 |
| --- | --- | --- |
| 記憶體 | 約 0.4 GB（紀錄器 0.2 到 0.35 GB） | 4 美元 |
| CPU | 約 0.2 到 0.4 vCPU | 4 到 8 美元 |
| 上傳到 Bucket 的流量 | 每天 2.5 到 5 GB，每 GB 0.05 美元 | 4 到 8 美元 |
| Bucket 儲存 | 兩週後約 35 到 75 GB | 1 美元以內 |
| 磁碟 | 5 GB | 1 美元以內 |

CPU 是估計值，還沒在 Railway 上量過。可以在 workspace 設花費硬上限；超過會停機，Bucket 也會暫停存取。

## 日常操作

```bash
railway logs --service polyweather            # 所有子程序的輸出
railway ssh --service polyweather             # 進容器
polyweather status                            # 進容器後：存檔概況
polyweather check streak                      # 連續幾天沒有缺口
polyweather station status                    # 有沒有測站被凍結
polyweather offload --backup                  # 立刻上傳並備份資料庫
```

## 還沒驗證的

- Docker 映像沒有在本機建過（開發環境沒有 Docker daemon）；只驗證過相同的安裝步驟在乾淨環境可以完成。
- 上傳只用模擬的 S3 測過，還沒對真的 Railway Bucket 跑過。上線後先看 `offload` 那一行日誌：`uploaded` 大於 0 才算通。
- `run-all` 在本機實跑過約 3 分鐘：首次啟動流程、兩個常駐程序、定時工作和收尾都正常。
