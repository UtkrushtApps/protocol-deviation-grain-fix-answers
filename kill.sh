#!/usr/bin/env bash
set +e

cd /root/task 2>/dev/null

echo "Stopping compose services..."
docker compose down || true

echo "Removing compose services and volumes..."
docker compose down -v || true

echo "Removing task-specific networks if any remain..."
docker network rm task_default protocol-deviation-grain-fix_default || true

echo "Removing task-specific images if any were built..."
docker image rm protocol-deviation-grain-fix || true

echo "Pruning leftover Docker resources..."
docker system prune -a --volumes -f || true

echo "Removing task working directory..."
rm -rf /root/task || true

echo "Cleanup completed successfully!"
