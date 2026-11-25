# polyweather

Polymarket 每日氣溫盤的紙上交易研究系統。只讀公開資料、只寫本機檔案，**沒有任何下單路徑**：沒有錢包、沒有簽章、沒有任何需要驗證的交易端點。

這個 repo 目前是規格裡的第一批：共用資料底座，加上結算值重建。紙上撮合、帳本、訊號與評估程式是下一批。

## 目前有什麼

| 元件 | 做什麼 | 指令 |
| --- | --- | --- |
| 盤口目錄 | 每 10 分鐘掃 Gamma，存 event、區間盤、費率、最小跳動；規則全文逐字比對模板，轉成 `ResolutionSpec` | `polyweather catalog scan` |
| 規則變更偵測 | 規則全文或結算來源一變，寫變更紀錄並凍結該測站 | 同上；`polyweather station status` |
| 訂單簿紀錄器 | WebSocket 原始訊息逐筆落地（附本機接收毫秒）、每 60 秒完整快照、REST 雜湊抽查、斷線記為 gap | `polyweather record` |
| 重播來源 | 照原始接收順序回放錄下的訊息，和即時來源同一個介面 | `polyweather.marketdata.replay` |
| METAR 存檔 | 每分鐘輪詢 aviationweather.gov 快取檔，每 10 分鐘用 API 補齊並記錄 AWC 收到時間 | `polyweather metar poll` |
| NBM 存檔 | NBP（百分位數）解析入庫；NBS、NBH 原文存檔 | `polyweather nbm archive` |
| LAMP 存檔 | 每小時逐時氣溫指引，1 到 38 小時 | `polyweather lamp archive` |
| 集成預報存檔 | Open-Meteo（主）或 ECMWF 開放資料直讀（備援），同一個 `ForecastSource` 介面 | `polyweather ensemble archive` |
| 價格歷史存檔 | 交易所的 1 分鐘價格序列，紀錄器有缺口時的備援 | `polyweather prices archive` |
| 結算值比對 | 依規則從 METAR 重建結算值，和實際結算比對；不一致就凍結測站 | `polyweather check settlement` |
| 每日缺口檢查 | 各存檔前一天是否齊全；連續 7 天全過是第一道關卡 | `polyweather check gaps`、`check streak` |
| 單一行程模式 | 家用電腦、容器等沒有 systemd 的環境用：一個指令跑兩個常駐程序和全部定時工作 | `polyweather run-all` |
| 資料庫備份 | 每天把資料庫完整複製一份、壓縮後放 `backup/`，保留 7 份 | `polyweather backup` |
| Bucket 上傳 | 寫完的原始檔上傳 S3 相容的 Bucket，過期的本機副本刪除，每天備份資料庫 | `polyweather offload` |
| M0 機率分布 | NBP 五個百分位數轉成連續分布，再積分到各區間 | `polyweather.models.m0` |

## 結算規則（已從規則全文確認）

2026-10-23 起，氣溫盤改用 aviationweather.gov 的 METAR 原始報文結算。規則全文是固定模板，只有測站、日期、網址不同。重點：

- 只計例行報告。行首是 `SPECI` 的不算；其他都算，包含不在整點的、沒有標籤的。
- 取備註裡的逐時溫度群（`T` 加八位數，十分之一 °C）。沒有這一群才用本文的整數 °C。
- 取測站當地日曆日（含日光節約時間）內的最高值或最低值。
- 華氏盤：°F = °C × 9/5 + 32，四捨五入到整數，剛好 .5 往上進。攝氏盤直接四捨五入。
- 同一觀測時間有多個版本時，以結算來源當下顯示的為準（更正報取代原報）。
- 來源資料明顯有誤時，盤口可延後最多 7 天再結算。這一條需要人判斷，程式不處理。

程式的做法是逐字比對：規則全文必須和 `src/polyweather/resolution/templates/` 裡四個模板之一完全相同，否則標為 `unparsed`，不進入任何策略。模板的語意寫在 `resolution/spec.py` 的 `TEMPLATE_RULES`。

回溯驗證（2026-10-23 到 11-23，全部已結算盤，共 2,778 個城市日）：

| 範圍 | 城市日 | 一致 | 不一致 | 一致率 |
| --- | --- | --- | --- | --- |
| 美國最高溫（11 站） | 352 | 352 | 0 | 100% |
| 美國最低溫 | 308 | 307 | 1 | 99.7% |
| 其他城市最高溫 | 1,183 | 1,173 | 9 | 99.2% |
| 其他城市最低溫 | 935 | 924 | 11 | 98.8% |

其他城市最高溫有 1 個城市日當天只有 18 筆報文，不到 20 筆不判定。

21 筆不一致都有解釋：

- 20 筆落在 10/23 到 10/25。AWC 只保留約 30 天，這三天要靠 IEM 回補，而 IEM 對半小時發報的測站只給整點報。
- 只用 AWC 自己的報文判定的 2,505 個城市日裡，不一致的只有 1 筆：邁阿密 11/7 最低溫。一筆遲到 27 分鐘的報文比前後低 1°C，盤口延後一週才結算，沒有採計那一筆。

比對時優先只用 AWC 的報文，前提是存檔涵蓋整個當地日；不足才併入其他來源。當天不到 20 筆例行報告就不判定，避免剛部署時因為存檔不完整而誤凍結。

## 快速開始

需要 Python 3.11 以上。

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest
```

```bash
export POLYWEATHER_DATA_DIR=./data
polyweather catalog scan --history
polyweather record --duration 120
polyweather status
```

`--history` 是匯入模式：第一次掃描時，已經發生過的規則變更不會凍結任何測站。

## 部署

三種方式，擇一：

- **家用電腦**（Windows、macOS、Linux；Docker 或直接跑）：見 [`deploy/HOME.md`](deploy/HOME.md)。最短的路是 `docker compose up -d --build`。
- **Railway**（單一容器，原始資料放 Bucket）：見 [`deploy/RAILWAY.md`](deploy/RAILWAY.md)。
- **自己的 Ubuntu 主機**（systemd）：見下一節。

前兩種跑的都是 `polyweather run-all`：一個指令啟動兩個常駐程序，並按時間表跑其餘的工作。

### Ubuntu + systemd

```bash
git clone https://github.com/KCL1104/poly_weather.git && cd poly_weather
sudo ./deploy/install.sh
```

腳本會建立 `polyweather` 使用者、裝到 `/opt/polyweather`、資料放 `/var/lib/polyweather`、設定寫在 `/etc/polyweather.env`，並啟用兩個常駐服務和七個排程。重跑是安全的：會更新程式，保留設定和資料。

| 單元 | 排程 |
| --- | --- |
| `polyweather-recorder.service` | 常駐，掛掉 5 秒後重啟 |
| `polyweather-metar.service` | 常駐 |
| `polyweather-catalog.timer` | 每 10 分鐘 |
| `polyweather-nbm.timer`、`polyweather-lamp.timer` | 每 10 分鐘，只抓還沒有的批次 |
| `polyweather-ensemble.timer` | 每小時 |
| `polyweather-prices.timer` | 每天 00:40 UTC |
| `polyweather-check-settlement.timer` | 每天 00:15、12:15 UTC |
| `polyweather-check-gaps.timer` | 每天 00:30 UTC，檢查前一天 |

任何單元失敗都會觸發 `deploy/alert.sh`。在 `/etc/polyweather.env` 填 `ALERT_WEBHOOK_URL`（Slack 或 Discord 的 incoming webhook）就會收到通知；沒填只寫進 journal。

安裝腳本和 systemd 單元通過了語法檢查（`bash -n`、`systemd-analyze verify`），但還沒有在真的主機上跑過。

### 磁碟

2026-11-24 11:10 UTC（美東清晨，偏安靜的時段）實測，範圍是美國 11 站的最高溫與最低溫盤，共 1,188 個 outcome：

| 項目 | 實測 | 換算 |
| --- | --- | --- |
| 訊息量 | 每秒約 440 則 | |
| 原始訊息 Parquet（zstd） | 4 分鐘 5.5 MB，壓縮比約 10 倍 | 每天約 2 GB |
| 60 秒快照 | 每份約 190 KB | 每天約 270 MB |

同一天 15:09 到 15:34 UTC（美東上午）再量一次：每秒 550 到 700 則，原始訊息換算每天 2.6 到 3.2 GB，快照每天 0.3 GB。

合起來每天約 2.3 到 3.5 GB，13 週約 210 到 320 GB；還沒量過美東下午到收盤的時段，估計時把上限放到每天 5 GB。建議先配 300 GB 以上的磁碟，第一週看實際用量再決定要不要調整。原始訊息裡佔空間的主要是每筆更新附的雜湊，壓不下去。

`POLYWEATHER_SCOPE=all` 會把 48 個測站全部納入，量大約是 4 到 5 倍。

### 集成預報的兩個來源

| | Open-Meteo（預設） | ECMWF 開放資料直讀（備援） |
| --- | --- | --- |
| 指令 | `ensemble archive --source openmeteo` | `ensemble archive --source ecmwf_open` |
| 每次下載量 | 一個請求，數百 KB | 每輪預報約 460 MB（0 到 72 小時、51 個成員） |
| 耗時 | 幾秒 | 順利時約 15 分鐘（推估）；S3 限流嚴重時，實測 2 個時效花了 9 分鐘 |
| 授權 | 免費方案限非商業用途 | CC-BY-4.0，可商用，需標示來源 |
| 回查 | 個別成員約 3 天 | 存檔回到 2024-01-31 |
| 額外安裝 | 無 | `pip install -e ".[ecmwf]"`（ecCodes） |

- 兩個來源對同一時刻的 11 站集成平均，相關係數 0.9986，平均相差 0.22°C，離散度幾乎相同。差異最大的是海岸測站（KLAX、KSFO），因為直讀取的是最近格點，沒有陸海遮罩。
- Open-Meteo 的額度按 IP 計。在共用出口的環境裡會遇到 429，自己的主機不會有這個問題。
- ECMWF 的檔案自 2026-05-12 起不含控制預報，成員 0 改取自 `oper`。兩者在切換前同一輪的 2 m 氣溫逐位元相同，但檔案本身沒有標示，這是推論。
- ECMWF 直讀沒有排進預設排程。排程單元的逾時是 15 分鐘，改用它之前要先調長。
- 回補一年的歷史成員（每天兩輪）約要 620 GB、連續下載一週左右。可行，但不是順手就能做的事。

## 交易所快照會落後一筆

2026-11-24 實跑時發現：同一個瞬間有兩筆變動（例如兩張掛單同時到期撤銷）時，交易所的訂單簿快照有時停在兩筆之間的狀態，時間戳記比那兩筆變動早 1 到 5 毫秒，要等這本簿子下一次變動才更新。REST 的 `/book` 和 WebSocket 訂閱時送來的 `book` 訊息都一樣。即時串流的 `price_change` 才是對的：查過的三個例子裡，同一價位之後的變動都和串流累計出來的數量吻合，和快照的數量不吻合。

對系統的影響：

- **紀錄器的抽查**：快照和本機的簿子對不上時，先看快照的時間戳記。快照不比串流最後一筆變動新，就判定是快照過期，保留本機的簿子，記在 `checks_stale_snapshot`。快照比較新才算串流漏訊息，重新訂閱並記 gap。修正前的做法會把對的簿子換成過期的快照。
- **重播與研究**：訂單簿要從 `raw/book` 的串流重建。剛連線時拿到的快照可能有少數幾本落後一筆，要到該價位下一次變動才會對。
- **頻率**：每分鐘抽查 40 本，修正前的 19 分鐘內遇到 3 次。修正後再跑 20 分鐘遇到 1 次（同一毫秒 4 筆撤單），紀錄器保留了本機的簿子；幾分鐘後交易所的快照更新，和本機的簿子逐價位一致。

## 資料放哪裡

```
$POLYWEATHER_DATA_DIR/
  db/polyweather.sqlite        盤口目錄、ResolutionSpec、變更紀錄、METAR、NBM、LAMP、
                               gap、凍結狀態、每日檢查結果
  raw/book/date=YYYY-MM-DD/    WebSocket 原始訊息（recv_ms, conn, seq, msg）
  raw/snapshots/date=.../      每 60 秒的完整訂單簿
  raw/ens/source=.../date=.../ 集成預報成員
  raw/prices_history/date=.../ 交易所價格序列
  state/recorder.json          紀錄器心跳，用來在重啟時補記 gap
  backup/                      資料庫的每日備份（`run-all` 沒接 Bucket 時），保留 7 份
```

用 Docker Compose 跑時，`db/` 在 Docker 的磁碟區裡，主機上看得到的資料庫是 `backup/` 那一份，原因見 [`deploy/HOME.md`](deploy/HOME.md)。

## 凍結與解凍

三種情況會凍結測站，並寫進 `pause_log`：

- 已上架盤口的規則全文或結算來源被改動。
- 同一系列的新盤口，規則解析結果和前一天不同（例如換了結算來源）。
- 結算值比對出現不一致。

解凍是人工步驟，而且要求凍結之後至少有一次比對一致：

```bash
polyweather station thaw KLGA --note "規則重讀過，語意沒變；11/25 比對一致"
```

## 測試

```bash
pytest
```

測試用的是 2026-11-24 抓下來的真實資料：一個已結算盤的完整規則與結果、該日的 METAR 原始報文、NBM 與 LAMP 公報片段。WebSocket 和紀錄器用假連線測試重連、心跳、gap 標記與雜湊抽查。

每次推送只要動到程式，GitHub Actions 會在 Ubuntu、Windows、macOS 上用 Python 3.11、3.12、3.13 各跑一次測試（`.github/workflows/tests.yml`）。

同一個檔案裡還有三個「實際啟動」的工作：在三種系統上分別用 `scripts/start.sh`、`scripts\start.bat` 和 `docker compose` 把整套系統對著真的資料來源跑 5 分鐘，再檢查錄到的東西。它們不會自動跑，要在 GitHub 的 Actions 頁面按 "Run workflow"，或是在 commit 訊息裡加上 `[live]`。

## 還沒做的

- 紙上撮合、五本帳、每日重播對帳（規格的「重建與模型」階段）。研究結果出來後決定先不蓋，見下方「研究紀錄」。
- M1、LAMP nowcast、訊號與部位規則、評估用的分析程式。
- 事件監控第二層（SubQ Snap），排在第二道關卡之後。

## 研究紀錄

動工前做了兩份研究，結果和限制寫在各自目錄的 README。

`research/retro/`：回溯研究。

- **NBM 的校準**（13 個月、11 站）：結算值平均比 NBM 中位數低 0.6°F；80% 區間涵蓋率 81%；誤差尾部比常態厚 1.7 到 1.9 倍。「各站平移加厚尾」是試過的做法裡最好的。模型給的機率超過 0.35 時仍然偏高。
- **M0 對市場價**（10/24 到 11/23，341 個美國城市日）：成功標準第 2 項的回溯版在三個決策時點都不顯著。市場隱含平均值的誤差是 1.0 到 1.3°F，校正後的 NBM 是 1.7 到 1.8°F。兩者意見不同時，對的是市場。
- **H3**：這一個月的熱門區間沒有被高估。
- **H2a**：區間被觀測值排除時，價格在 AWC 發布報文之前就已經歸零。

`research/forecast_test/`：純預報檢驗。NBS、NBP、LAMP、GFS MOS、HRRR 加觀測的最佳組合，誤差是 1.42 到 1.73°F，三個決策時點都比市場差 0.40 到 0.45°F，也沒有市場價以外的資訊。加入 ECMWF 只再少 0.02 到 0.03°F；直接在檢驗月份上擬合的上界也還差 0.3°F 以上。

這些都是回溯結果，用的是沒有深度的中間價，不是正式評估。但五本帳對應的假說目前沒有一個有支持的證據，所以帳本先不蓋。
