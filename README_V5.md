# README_V5

## The Purpose

- 本版本为“报表链路完整版”的逻辑备份。
- 记录了已验证的单机性能优化方案、秒级搜索链路与云端双模配置逻辑，作为 V5-Pro 持续迭代时的安全底库。

## Use Cases

- 场景 A：V5-Pro 开发出现重大逻辑崩塌时，回滚参考当前 V5 的成熟提取算法。
- 场景 B：在本地局域网环境下，需要极速查询历史报表且不依赖分布式架构时使用。

## Quick Start & Settings

1. 创建 Python 虚拟环境，并执行 `pip install -r requirements.txt`。
2. 打开根目录 `.env` 文件，参考 `.env.example`，必须将 `GATEWAY_IP` 修改为当前本地网关地址。
3. 填入有效的 `GEMINI_API_KEY`。

注意事项：

- 本版本不含数据库文件，相关 `.db` 文件已加入 ignore。
- 首次运行会自动初始化空库，需重新同步数据。

## Desensitization Check

- 已确认将代码中硬编码的 `192.168.x.x` 等私有 IP 替换为 `os.getenv` 动态加载或中性占位示例。
- 当前运行入口会优先读取 `GATEWAY_IP`，未配置时回退到 `PROXY_HOST` 默认值。
