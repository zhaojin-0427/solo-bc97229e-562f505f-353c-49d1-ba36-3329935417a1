#!/bin/sh
# 角色入口：harvester | mock-a | mock-b
set -e

# 可选：让 mock 以「持续慢发正文」模式启动（演示/测试取消中断）
SLOW_ARGS=""
if [ -n "${MOCK_SLOW_BODY:-}" ]; then
  SLOW_ARGS="--slow-body --slow-chunk-seconds ${MOCK_SLOW_CHUNK_SECONDS:-0.3}"
fi

case "$1" in
  harvester)
    # 单 worker + 多线程：采集任务是进程内后台线程，必须保证单进程
    exec gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 --timeout 120 \
      "harvester.app:create_app()"
    ;;
  mock-a)
    exec python -m mock_oai.server --code A --port 8081 \
      --seed 101 --count 40 --registrant 1001 --page-size 10 $SLOW_ARGS
    ;;
  mock-b)
    # 前 15 条 DOI 与 A 共用（不同写法/大小写），用于演示跨源去重
    exec python -m mock_oai.server --code B --port 8082 \
      --seed 202 --count 45 --registrant 2002 --page-size 12 \
      --overlap 15 --overlap-registrant 1001 $SLOW_ARGS
    ;;
  *)
    exec "$@"
    ;;
esac
