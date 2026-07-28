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
