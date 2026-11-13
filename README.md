# 双机构库 OAI-PMH 采集去重目录系统

高校图书馆数据员工具：把两家机构知识库通过 **OAI-PMH 2.0（ListRecords / oai_dc）**
全量采集到本地 **SQLite**，按归一化 DOI 去重合并成可检索网页目录，由 **Flask** 提供
采集控制台与查询入口。附带两个可做故障注入的本地模拟机构库，以及一键
**Docker Compose** 演示环境。

## 一、快速开始（Docker Compose）

```bash
docker compose up --build
```

启动后三个服务：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集 Web 入口 | http://localhost:8000 | 控制台、进度、DOI 目录 |
| 模拟机构库 A（优先级 1） | http://localhost:8081/panel | 故障注入控制台 |
| 模拟机构库 B（优先级 2） | http://localhost:8082/panel | 故障注入控制台 |

打开 http://localhost:8000 ，点击 **「并行启动两源采集」**，几秒后刷新即可看到
两个来源的完整轮次与合并目录。模拟数据中 B 的前 15 个 DOI 与 A 重复（但 DOI 写法、
大小写和题名不同），用于演示去重与出处合并。

一条命令走完「采集 → 慢正文取消本轮 → 断流续采 → 令牌失效重采 → 撤稿接替」演示：

```bash
./demo.sh
```

> 没有 Docker 时可用 `./run_local.sh`（需要 Python 3.11，依赖安装见文末附录）。

## 二、需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 配置两个 OAI-PMH 地址及优先级 | `HARVESTER_SOURCES` 环境变量（JSON），写入 `sources` 表，priority 数字越小越优先 |
| 全量采集 ListRecords 的 oai_dc | `harvester/oai.py`：逐页 `ListRecords`，解析 identifier / title / DOI / deleted |
| 每源 DOI 唯一、单轮内容固定 | 模拟库保证单源 DOI 唯一；采集按 resumptionToken 翻完整个列表 |
| DOI 去 `https://doi.org/` 前缀并忽略大小写 | `harvester/doi_utils.py`：兼容 http/https、dx.doi.org、`doi:`、裸 DOI，统一小写为合并键 |
| 保留标识、题名和 DOI，展示全部出处 | `catalog_records` 保留每个来源的 OAI 标识、原始 DOI 值、题名；DOI 详情页列出全部出处（含撤稿记录） |
| 采用优先来源题名 | SQL 视图 `merged_catalog` 用窗口函数按 `priority` 取首选题名 |
| deleted 仅撤本来源，其他来源接替 | 撤稿记录按来源独立落库（`is_deleted=1`），合并视图只排除被撤的那一行；该 DOI 仍由其他来源在架并提供题名 |
| 两源并行采集 | 每源独立后台线程（`HarvestManager`），互不阻塞 |
| 同源重复启动复用任务 | 线程存活时重复点击只返回 `reused`，不会开第二个任务 |
| 完整一轮后才替换该源目录 | 只有最后一页（无 resumptionToken）才在单事务内 `DELETE + INSERT` 本来源快照 |
| 合并保留另一源同期更新 | 交换只按 `source_id` 删本来源行，另一来源目录原样保留 |
| 查询只见完整状态 | 查询全部走正式目录 `catalog_records` / `merged_catalog`；半截数据只在 `staging_records` |
| **取消本轮采集** | 控制台按「来源 + 轮次」取消；受理后阻止该轮发布并**立即硬中断在途 HTTP 响应**（socket shutdown，不等慢正文结束或读超时）；采集退出、暂存与令牌清理完成后才显示「已取消」 |
| 取消与发布互斥 | `request_cancel` 与 `complete_round` 均为 `BEGIN IMMEDIATE` 写事务，SQLite 写锁保证只有一方先提交；发布先提交则取消被拒绝 |
| 重复取消返回原结果 | 同一轮次的重复取消幂等返回（cancelling → cancelled）；旧页面的轮次号与新轮不匹配时拒绝（`rejected_stale_round`） |
| 取消中拒绝同源启动 | `cancelling` 状态下启动返回 `cancelling`（HTTP 409）；收尾完成后可从首页新采；另一源采集与目录查询不受影响 |
| 取消意图持久化 | `cancelling` 状态落库；清理期间进程重启会在启动时**继续完成取消**，绝不转成可续采的 error |
| 迟到响应不改新轮 | 所有采集写入按「轮次 + running 状态」守卫：迟到的落盘/失败标记/发布一律丢弃 |
| 逐页持久化记录与 resumptionToken，重启续采 | 每页一个短事务落盘记录、序号与令牌；进程重启后状态置为 error，点击续采从令牌页继续 |
| badResumptionToken 清空本轮暂存从头重采 | `restart_from_beginning()`：删本轮 staging、round+1、令牌清空，正式目录不动；重采次数有上限保护 |
| 网络失败可续采 | 网络异常只标记 `error/network`，已落盘的页与令牌保留，续采沿用同一 round |
| 等待响应不占库写事务 | HTTP 请求在事务之外发起，拿到响应后才开短事务写库 |
| 旧目录仍可查 | 失败轮次不触碰 `catalog_records`，WAL 模式下读写不互斥 |
| 网页可启动、查看进度、按 DOI 查目录 | `/` 控制台、`/catalog` 检索、`/doi/<key>` 出处详情，另附 JSON API |
| 可使令牌过期、断流、撤稿的本地双源 | `mock_oai/`：两个标准库实现的 OAI 服务，`/panel` 页面一键注入三类故障 |

## 三、Web 入口与 API

| 路径 | 方法 | 作用 |
| --- | --- | --- |
| `/` | GET | 采集控制台：每源状态、进度条、令牌、错误信息、启动/续采按钮 |
| `/catalog?q=&page=` | GET | DOI 目录检索（DOI 或题名关键词，DOI 不区分大小写、可带前缀） |
| `/doi/<doi_key>` | GET | 单条 DOI 的全部出处、优先题名、撤稿状态 |
| `/no-doi` | GET | 无 DOI 记录清单（保留但不参与合并） |
| `/harvest/start` | POST | 表单 `source_id` 启动/续采单源；不带参数并行启动两源；取消收尾中返回 409 |
| `/harvest/cancel` | POST | 表单 `source_id` + `round_id` 取消指定轮次；重复取消返回原结果；发布已提交或轮次不匹配返回 409 |
| `/api/sources` | GET | 两源采集状态 JSON |
| `/api/stats` | GET | 目录统计 JSON |
| `/api/catalog?q=&limit=&offset=` | GET | 合并目录 JSON |
| `/api/lookup?doi=...` | GET | 按 DOI 精确查询（自动归一化），返回首选题名与全部出处 |
| `/healthz` | GET | 健康检查 |

## 四、故障注入控制台（模拟机构库）

每个模拟源的 `/panel` 提供：

- **让下一次翻页令牌过期（一次性）**：下一次带 resumptionToken 的请求返回
  `badResumptionToken`，触发采集器清空本轮暂存并从首页重采；
- **让令牌持续过期**：每次翻页都失败，用于观察重采上限保护与「旧目录不变」；
- **让下一次翻页断流（HTTP 500）**：模拟网络中断，采集器停在 error 并保留断点，续采即可继续；
- **让下一次响应慢正文 / 持续慢正文模式**：响应头立即返回，正文按 256 字节小块
  滴流发送（默认每块 0.3s，可用 `--slow-chunk-seconds/--slow-chunk-bytes` 调整），
  用于演示「取消立即中断在途响应，不等慢正文结束或读超时」；
  也可用 `MOCK_SLOW_BODY=1 docker compose up` 让 mock 启动即进入持续慢正文模式；
- **撤下 / 恢复记录**：按 OAI 标识注入或撤销 `status=deleted`；
- **恢复数据集**：还原初始数据。

## 五、数据库与并发模型

```
sources            来源配置（id、code、名称、URL、优先级）
harvest_state      每源状态机：idle/running/error/success、round、令牌、页码进度
staging_records    本轮暂存（逐页写入，重启续采 / badToken 重采都基于它）
catalog_records    正式目录快照（查询入口，完整轮次事务交换）
merged_catalog     合并视图：归一化 DOI 分组 + 优先来源题名 + 出处列表
```

- 采集线程每个持有独立 SQLite 连接，WAL + `busy_timeout` 保证读写并发；
- gunicorn 固定 **1 worker 多线程**：采集任务是进程内后台线程，多进程会破坏任务复用；
- 状态机转移：`idle → running → success/error/cancelling`；`cancelling → cancelled`；
  error 且暂存在 → 续采，否则（含 success/cancelled 后手动重采）→ 新开一轮并清空旧暂存。

### 取消一轮采集的完整生命周期

1. 控制台提交「来源 + 轮次」→ `request_cancel` 在单个写事务内把 `running`
   置为 `cancelling`（与 `complete_round` 互斥，发布先提交则拒绝取消）；
2. `HarvestManager` 找到该轮次的 `CancelToken`，置位事件并 **shutdown 在途
   响应的底层 socket**，阻塞中的慢正文读取立即失败，不等正文结束或读超时；
3. 采集线程退出抓取循环后执行 `finalize_cancel`：删除本轮暂存、清空令牌，
   状态才置为 `cancelled`（页面此时才显示「已取消」）；
4. `cancelling` 期间同源启动被拒绝；`cancelled` 后可从首页新采；
5. 进程在清理期间重启：启动恢复发现 `cancelling` 会继续完成第 3 步，
   不会变成可续采的 error；`running` 残留仍按原逻辑转为 error 供续采。

## 六、测试

```bash
PYTHONPATH=.pydeps:. python3 -m unittest discover -s tests -v
```

36 个用例覆盖：DOI 归一化与 OAI 解析、全量采集与双源去重、并行与任务复用、
撤稿接替、整轮原子交换与查询可见性、网络断点续采、badResumptionToken 清空重采、
进程重启续采、失败轮次旧目录完好，以及取消专题（`tests/test_cancel.py`）：
慢正文中断时序、取消/发布双向互斥（含线程级交错）、重复取消幂等、旧页面
不能取消新轮、取消中拒绝同源启动、清理期间重启继续取消、迟到写入不改新轮。

## 附录：不使用 Docker 的本地运行

```bash
python3 -m pip install --target=.pydeps -r requirements.txt   # 或自建 venv
./run_local.sh            # 同时启动两个 mock 与 Flask（端口 8081/8082/8000）
```
