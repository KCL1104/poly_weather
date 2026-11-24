# 在家用電腦上跑

整套系統是一個指令：`polyweather run-all`。它會啟動訂單簿紀錄器和 METAR 輪詢，並按時間表跑其餘的工作（盤口目錄、NBM、LAMP、集成預報、價格備份、每日檢查、資料庫備份）。Windows、macOS、Linux 都能跑，建議用 Docker，因為三種系統上跑的是同一個容器，這條路徑實際測過。

系統只讀公開資料，沒有錢包、不簽章、不下單。

## 需要什麼

| 項目 | 需求 |
| --- | --- |
| 電腦 | 能一直開著、不進入睡眠 |
| 磁碟 | 美國 11 站每天約 2.5 到 5 GB。兩週約 35 到 75 GB，13 週約 230 到 490 GB |
| 記憶體 | 1 GB 就夠（紀錄器實測約 0.2 GB） |
| 網路 | 一般家用網路。要能連到 Polymarket 的公開資料 API 和美國的氣象資料站 |
| 時間 | 系統的自動對時要開著（預設就是開的）。收到訊息的時間是用本機時鐘記的 |

不要透過 VPN 或代理跑。規格不允許繞過地區限制；如果家裡的網路讀不到 Polymarket 的公開資料，就改用雲端主機，不要繞。

## 方式一：Docker（建議）

1. 安裝 Docker。Windows 和 macOS 裝 [Docker Desktop](https://www.docker.com/products/docker-desktop/)，Linux 裝 Docker Engine。
2. 取得程式：

   ```bash
   git clone https://github.com/KCL1104/poly_weather.git
   ```

   ```bash
   cd poly_weather
   ```

   程式合併進 `main` 之前，clone 時要加 `-b claude/confident-galileo-xzx4ma`。
3. 啟動：

   ```bash
   docker compose up -d --build
   ```

第一次啟動會先匯入盤口目錄和最近 4 天的 METAR，大約 2 分鐘，之後紀錄器才開始錄。

### 常用指令

| 要做什麼 | 指令 |
| --- | --- |
| 看現在的狀態 | `docker compose exec polyweather polyweather status` |
| 看日誌 | `docker compose logs -f --tail 100` |
| 看容器是否健康 | `docker compose ps`（狀態欄會顯示 `healthy`） |
| 停止（資料保留） | `docker compose down` |
| 更新程式 | `git pull`，再跑一次 `docker compose up -d --build` |

### 開機自動啟動

容器設定成「除非手動停止，否則自動重啟」，所以只要 Docker 本身開機會啟動就行：

- **Windows、macOS**：Docker Desktop 的 Settings → General，勾選 "Start Docker Desktop when you sign in"。電腦重開後要登入一次。
- **Linux**：`sudo systemctl enable docker`。

### 資料在哪裡

| 位置 | 內容 |
| --- | --- |
| `data/raw/` | 所有原始存檔（Parquet），佔空間的主要是 `book_raw` |
| `data/backup/` | 資料庫的每日備份，保留最近 7 份 |
| `data/state/` | 執行中的小檔案，不用管 |
| Docker 磁碟區 `polyweather_db` | 正在使用的資料庫 |

資料庫不放在 `data/` 裡，是因為 SQLite 的寫入方式在 Windows、macOS 分享給容器的資料夾上不安全。要讀資料庫、搬到別台電腦或另外備份，用 `data/backup/` 裡最新的那份。想立刻產生一份：

```bash
docker compose exec polyweather polyweather backup
```

**不要執行 `docker compose down -v`**：`-v` 會把磁碟區連同資料庫一起刪掉。

## 方式二：不用 Docker

需要 Python 3.11 以上。

- **macOS、Linux**：

  ```bash
  scripts/start.sh
  ```

- **Windows**：執行 `scripts\start.bat`。

第一次會自己建立虛擬環境並安裝，之後直接啟動；按 Ctrl+C 停止。資料全部在 `data/` 底下，資料庫是 `data/db/polyweather.sqlite`。

這個方式要自己處理開機啟動。Ubuntu 可以改用 `sudo ./deploy/install.sh`，它會裝成 systemd 服務（見 README）。Windows 可以用「工作排程器」建立一個「登入時」執行 `scripts\start.bat` 的工作。

其他指令在虛擬環境裡：macOS、Linux 是 `.venv/bin/polyweather status`，Windows 是 `.venv\Scripts\polyweather status`。

## 設定

都有預設值，不設也能跑。要改的話，把 `.env.example` 複製成 `.env` 再編輯：

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `POLYWEATHER_DATA_DIR` | `./data` | 存檔位置。磁碟不夠時指到另一顆，例如 `D:/polyweather` |
| `POLYWEATHER_SCOPE` | `us` | `all` 會錄全部 48 站，量大約是 4 到 5 倍 |
| `ALERT_WEBHOOK_URL` | 空 | Slack 或 Discord 的 incoming webhook。填了之後，程序掛掉、工作失敗、磁碟超過 85% 都會通知 |

`.env` 不會進 git。`scripts\start.bat` 不讀 `.env`，Windows 不用 Docker 時要改用系統的環境變數。

## 不要讓電腦睡著

電腦睡眠期間什麼都錄不到。醒來後系統會自己重連，缺口會記在資料庫的 `gaps` 表，但那段訂單簿補不回來。

- **Windows**：設定 → 系統 → 電源 → 螢幕與睡眠，把「讓裝置進入睡眠狀態」設成「永不」。
- **macOS**：系統設定 → 電池（或「能源」）→ 選項，打開「顯示器關閉時避免自動進入睡眠」。筆電要接著電源、不要闔上。
- **Linux**：`sudo systemctl mask sleep.target suspend.target hibernate.target`。

## 每天看什麼

跑 `status`，看這幾項：

| 欄位 | 正常的樣子 |
| --- | --- |
| `recorder_running` | `true` |
| `last_new_metar_min_ago` | 小於 70（各站大約每小時一報） |
| `disk.used_share` | 低於 0.85 |
| `frozen_stations` | 空的。有測站被凍結代表結算規則或結算值對不上，要人工看過才解凍 |
| `last_jobs` | 每個工作的 `ok` 都是 `true` |

每天 00:30 UTC 會自動檢查前一天的存檔有沒有缺口，結果在日誌裡，也會寫進資料庫的 `daily_checks` 表。

## 驗證到哪裡

2026-11-24 在 Linux 上實際跑過：

- 用 `Dockerfile` 建出映像，用 `compose.yaml` 啟動。
- 首次啟動的匯入、紀錄器、METAR 輪詢、定時工作、健康檢查。
- 停止後再啟動，資料庫和存檔都還在，中斷的時段有記成缺口。
- `scripts/start.sh` 從全新的 clone 開始，自己安裝並啟動。

還沒驗證：

- **Windows 和 macOS 上的實際執行。** Docker 方式在三種系統上跑的是同一個 Linux 容器，風險低。`scripts\start.bat` 和 Windows 上的停止處理是照文件寫的，沒有在實機上跑過。
- **從台灣的網路連線。** 測試是在美國的雲端主機上做的。
- **連續跑好幾天。** 最長的一次測試是十幾分鐘。
