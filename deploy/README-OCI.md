# OCI 分片部署

本目录的 unit 文件用于 Oracle Linux 的 Always Free 实例。每台实例使用
自己的数据库副本和 `state/shard-N.sqlite`，不会并发写同一个 SQLite 文件。

## 首次安装

```sh
sudo dnf install -y python3 python3-pip sqlite rsync
sudo mkdir -p /opt/lexora/{build,state,tools}
sudo chown -R opc:opc /opt/lexora
python3 -m pip install --user httpx
sudo install -m 0644 deploy/lexora-enrich@.service /etc/systemd/system/lexora-enrich@.service
sudo systemctl daemon-reload
```

把 `build/lexora-open-oxford-scope.sqlite` 和 `tools/` 同步到 `/opt/lexora/` 后，
分别启用 `lexora-enrich@0` 至 `lexora-enrich@3`。由于 OCI A1 容量可能暂时不足，
实例创建失败时不要切换到收费规格；等待容量恢复后按同样配置重试即可。

## 进度和日志

```sh
python3 /opt/lexora/tools/oci_progress.py \
  --dataset /opt/lexora/build/lexora-open-oxford-scope.sqlite \
  --state /opt/lexora/state/shard-0.sqlite
journalctl -u lexora-enrich@0 -f
```

完成后将各副本同步回构建机，用 `merge_enrichment_shards.py` 合并，然后重建
FTS、20k 快照、manifest、覆盖率和 SHA-256。不要把私钥、状态库中的个人信息
或未获许可的 Oxford/OED 数据提交到公开仓库。

## E2.1.Micro Always Free 备用方案

Singapore 的 A1 容量不足时，可以使用控制台明确标注“符合始终免费条件”的两台
`VM.Standard.E2.1.Micro`，每台处理一个分片。微型实例内存很小，先在实例本地
引导盘建立 2GB swap，再安装依赖；这不会创建额外云磁盘。

每台实例安装 `deploy/lexora-enrich-micro@.service`，第一台启动分片 `0`，第二台
启动分片 `1`。该模板固定 `--shard-count 2 --workers 8`，请求会由 Cloudflare
中转层合并为每批最多 8 个词条，在不增加免费实例数量的前提下提高吞吐量。

查询中转使用 `deploy/lexora-lexicon-micro.service`，由 Cloudflare Worker
`worker/dictionary-edge.js` 通过 `dict.12323456.xyz` 访问。源站必须设置
`LEXORA_ORIGIN_TOKEN`，Worker 使用同名 secret；不要把令牌写入仓库。

同时启用 `lexora-enrich-watch@分片.timer`。它每 10 分钟检查一次持久化进度，
连续 45 分钟没有新记录时自动重启对应采集进程。状态库、数据集和 `.env`
均保存在实例引导盘中，系统重启后采集会从未完成词条继续。
