#!/bin/bash
# 同时创建 5 个目录同步任务，验证公平分配
# 用法: bash test_multi_job_fairness.sh <API_BASE_URL>
# 示例: bash test_multi_job_fairness.sh http://localhost:8080/api

API_BASE="${1:-http://localhost:8080/api}"
METADATA_ID="${2:-1}"  # 默认使用 id=1 的 metadata

if [ $# -lt 1 ]; then
    echo "用法: bash $0 <API_BASE_URL> [metadata_id]"
    echo "示例: bash $0 http://localhost:8080/api 1"
    exit 1
fi

echo "=========================================="
echo "  多 Job 公平分配测试"
echo "  API: $API_BASE"
echo "  Metadata ID: $METADATA_ID"
echo "=========================================="

# 创建 5 个 Job 的 payload
create_job() {
    local job_num=$1
    local src_dir="test-fairness/job${job_num}/"
    local dst_dir="test-fairness-dest/job${job_num}/"

    cat <<EOF
{
    "job": {
        "src_dir": "${src_dir}",
        "dst_dir": "${dst_dir}",
        "include": "",
        "exclude": "",
        "delete_source": false,
        "is_incremental": false,
        "periodic_interval": 0,
        "metadata_id": ${METADATA_ID}
    },
    "tasks": []
}
EOF
}

# 并发创建 5 个 Job
echo ""
echo "[1/3] 同时创建 5 个目录同步任务..."
JOB_IDS=()
for i in 1 2 3 4 5; do
    PAYLOAD=$(create_job $i)
    RESP=$(curl -s -X POST "${API_BASE}/jobs/" \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD")
    JOB_ID=$(echo "$RESP" | grep -o '"job_id":[0-9]*' | grep -o '[0-9]*')
    if [ -n "$JOB_ID" ]; then
        JOB_IDS+=($JOB_ID)
        echo "  Job $i 创建成功: ID=$JOB_ID (src: test-fairness/job${i}/)"
    else
        echo "  Job $i 创建失败: $RESP"
    fi
done

if [ ${#JOB_IDS[@]} -lt 2 ]; then
    echo "ERROR: 至少需要 2 个 Job 才能测试公平分配"
    exit 1
fi

echo ""
echo "[2/3] 等待 30 秒，让 Scanner 扫描并创建任务..."
sleep 30

echo ""
echo "[3/3] 监控各 Job 的任务分配情况 (每 10 秒采样一次，共 60 秒)..."
echo ""
printf "%-8s %-10s %-10s %-10s %-10s %-12s %-12s\n" "时间" "JobID" "Total" "Pending" "Success" "Failed" "SuccessRate"
printf "%-8s %-10s %-10s %-10s %-10s %-12s %-12s\n" "----" "-----" "-----" "-------" "-------" "------" "-----------"

# 采样 6 次
for sample in 1 2 3 4 5 6; do
    TIMESTAMP=$(date +"%H:%M:%S")
    for jid in "${JOB_IDS[@]}"; do
        RESP=$(curl -s "${API_BASE}/jobs/${jid}")
        TOTAL=$(echo "$RESP" | grep -o '"total_count":[0-9]*' | grep -o '[0-9]*')
        PENDING=$(echo "$RESP" | grep -o '"pending_count":[0-9]*' | grep -o '[0-9]*')
        SUCCESS=$(echo "$RESP" | grep -o '"success_count":[0-9]*' | grep -o '[0-9]*')
        FAILED=$(echo "$RESP" | grep -o '"failed_count":[0-9]*' | grep -o '[0-9]*')

        TOTAL=${TOTAL:-0}
        PENDING=${PENDING:-0}
        SUCCESS=${SUCCESS:-0}
        FAILED=${FAILED:-0}

        if [ "$TOTAL" -gt 0 ]; then
            RATE=$(echo "scale=1; $SUCCESS * 100 / $TOTAL" | bc 2>/dev/null || echo "0")
        else
            RATE="0"
        fi

        printf "%-8s %-10s %-10s %-10s %-10s %-12s %-12s\n" "$TIMESTAMP" "$jid" "$TOTAL" "$PENDING" "$SUCCESS" "$FAILED" "${RATE}%"
    done
    echo ""
    if [ $sample -lt 6 ]; then
        sleep 10
    fi
done

echo "=========================================="
echo "  测试完成"
echo "=========================================="
echo ""
echo "公平性分析:"
echo "  - 如果每个 Job 的 Success 数量接近，说明公平分配生效"
echo "  - 如果某个 Job 的 Success 一直为 0，说明该 Job 没有得到 worker 资源"
echo ""
echo "清理测试 Job:"
for jid in "${JOB_IDS[@]}"; do
    echo "  删除 Job $jid: curl -X DELETE ${API_BASE}/jobs/${jid}"
done