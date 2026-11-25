# 在家用電腦上跑

整套系統是一個指令：`polyweather run-all`。它會啟動訂單簿紀錄器和 METAR 輪詢，並按時間表跑其餘的工作（盤口目錄、NBM、LAMP、集成預報、價格備份、每日檢查、資料庫備份）。Windows、macOS、Linux 都能跑，兩種方式都在實機上驗證過（見最後一節）。建議用 Docker，因為開機自動啟動最省事。

目前這套只收資料和做檢查，不做任何紙上下單，也沒有會自己調整的策略。

系統只讀公開資料，沒有錢包、不簽章、不下單。

## 需要什麼

| 項目 | 需求 |
| --- | --- |
| 電腦 | 能一直開著、不進入睡眠 |
| 磁碟 | 美國 11 站每天約 2.5 到 5 GB。兩週約 35 到 75 GB，13 週約 230 到 490 GB |
| 記憶體 | 1 GB 就夠（實測整套約 0.35 到 0.4 GB，其中紀錄器 0.25 到 0.3 GB，跑一小時沒有持續增加） |
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

電腦睡眠期間什麼都錄不到。醒來後系統會自己重連，缺口會記在資料庫的 `gaps` 表。各種資料的情況不同：

| 資料 | 中斷之後 |
| --- | --- |
| 訂單簿 | 補不回來。每日的價格備份只有 1 分鐘一筆的中間價，沒有深度 |
| METAR | 中斷超過 2.5 小時，恢復時會自動把缺的那段重新抓回來（來源保留約 30 天） |
| NBM、LAMP | 每次執行都會補齊最近 30 到 40 小時還沒抓的批次。中斷更久的要手動跑 `polyweather nbm backfill` |
| 結算比對 | 當天的 METAR 有超過 3 小時的空檔就不判定，不會因為資料不全而誤凍結測站 |

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

每天 00:30 UTC 會自動檢查前一天的存檔有沒有缺口，結果在日誌裡，也會寫進資料庫的 `daily_checks` 表。啟動後的第一次檢查會回報不齊全（日誌裡是 `job check-gaps failed with exit code 2`），因為前一天還沒開始錄，這是正常的；從第二次起才有意義。

## 驗證到哪裡

2026-11-25 做了一次完整驗證。所有檢查都從 GitHub 重新 clone 的程式出發。

### 安裝與測試

| 項目 | 結果 |
| --- | --- |
| 157 項自動測試，Linux 上 Python 3.11、3.12、3.13、3.14 | 全過 |
| 同一批測試，GitHub 的 Ubuntu、Windows、macOS 實機，Python 3.11 到 3.13（Ubuntu 另加 3.14），共 10 組 | 全過 |
| 相依套件在 Windows、macOS（Intel 與 Apple 晶片）、Linux ARM 的現成安裝檔 | 都有 |

### 實際啟動（GitHub 的實機，對真的資料來源各跑 5 分鐘）

| 系統與方式 | 結果 |
| --- | --- |
| Windows，`scripts\start.bat` | 通過 |
| macOS，`scripts/start.sh` | 通過 |
| Ubuntu，`scripts/start.sh` | 通過 |
| Ubuntu，`docker compose up -d --build`（沒有改過的 `Dockerfile`） | 通過，含停止後再啟動 |
| Ubuntu，`sudo ./deploy/install.sh`（systemd） | 通過：兩個服務在跑、七個排程都排上、再跑一次安裝腳本會重啟服務 |

每一個都檢查：目錄與 METAR 有匯入、訂單簿有錄到而且檔案讀得出來、沒有寫到一半的檔案、資料庫完好、停止時紀錄器有把手上的資料寫完。

### 長時間實跑（Docker，72 分鐘）

| 項目 | 結果 |
| --- | --- |
| 錄到的訊息 | 254 萬筆，計數器沒有跳號，全部可解析 |
| 紀錄器自己的抽查 | 2,698 次，沒有任何一次判定串流漏訊息，零斷線 |
| 重播 | 69 份快照、81,972 本簿子，用錄下的串流全部重現 |
| 和交易所全量比對 | 11 次、每次 1,188 本。不同的只有交易所快照落後的情況，見 README |
| METAR | 期間來源列出的 21 則報文都在存檔裡，觀測後 3 到 4 分鐘入檔 |
| 排程工作 | 目錄 8 次、NBM 7 次、LAMP 8 次、集成預報 1 次，全部成功 |
| 記憶體 | 紀錄器 0.25 到 0.3 GB，整個容器不到 0.4 GB，沒有持續增加 |
| 存檔大小 | 每小時約 112 MB，換算每天 2.7 GB（美東晚間） |

### 故障注入

| 狀況 | 結果 |
| --- | --- |
| 紀錄器被強制結束 | 2 秒後自動重啟，發出告警，缺口有記錄 |
| 整個容器凍結 150 秒（模擬電腦睡眠） | 恢復後 13 秒內三條連線全部重連，163 秒的缺口有記錄 |
| 容器被強制關掉（模擬斷電）再啟動 | 資料庫完好，沒有寫到一半的檔案。缺口從最後一筆落地的資料算起 |
| Docker 本身重啟（模擬重開機） | 容器自己回來，不用下指令 |
| `docker compose down` 再 `up` | 資料庫與存檔都在 |
| 磁碟寫滿 | 87% 時先告警；寫滿後紀錄器反覆重啟並告警；清出空間後 5 秒內自己恢復，資料庫完好 |
| 終端機按 Ctrl+C | 1 秒內乾淨結束，沒有殘留的程序 |
| METAR 中斷 7 小時 | 重新啟動後 10 秒內把缺的報文補齊 |

### 驗證過程中發現並修正的問題

| 問題 | 會造成什麼 | 修正 |
| --- | --- | --- |
| 結算比對拿不完整的一天來判定 | 電腦停過幾小時後，隔天可能誤判並凍結測站 | 當天有超過 3 小時的空檔就不判定 |
| METAR 中斷超過 3 小時後不會回頭補 | 存檔留下永久的空檔 | 恢復時自動補抓整段 |
| 硬關機前最後不到 30 秒的訂單簿遺失卻沒標成缺口 | 研究時會把那段誤認成沒有變動 | 缺口改從最後一筆落地的資料算起 |
| Windows 上檔案被防毒軟體短暫鎖住時改名會失敗 | 紀錄器偶爾當掉重啟 | 改名失敗會重試；心跳檔寫不進去不再影響紀錄器 |
| `deploy/install.sh` 更新程式後沒有重啟服務 | 更新不會生效 | 更新後重啟兩個常駐服務 |
| 磁碟寫滿時留下寫到一半的檔案 | 佔用空間 | 啟動時清掉舊的殘檔 |

### 還沒驗證

- **從台灣的網路連線。** 所有實跑都在美國的機器上。
- **連續跑好幾天。** 最長一次是 72 分鐘。排在 UTC 凌晨的工作（價格備份、結算比對、缺口檢查、資料庫備份）是各自單獨跑過的。
- **Windows、macOS 上的 Docker Desktop。** 容器的內容和 Linux 上驗證的完全相同，但 Docker Desktop 本身沒有測到。
- **真的斷電。** 測的是強制結束程序；檔案系統層級的損壞沒有模擬。
- **Windows 上不開視窗的執行方式**（例如工作排程器設成「不論使用者是否登入」）。這種情況下停止時可能來不及把最後 30 秒寫完，缺口會照樣記錄。
