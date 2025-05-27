# 量化交易项目结构文档

## 项目概述
这是一个量化交易项目，包含数据获取、策略实现、数据库管理等功能模块。

## 目录结构

### 根目录
- `requirements.txt`: 项目依赖包列表
- `README.md`: 项目说明文档
- `.gitignore`: Git忽略文件配置

### utils/ - 工具类模块
- `data_fetcher.py`: 数据获取工具
- `tushare_loader.py`: Tushare数据加载器
- `db_manager.py`: 数据库管理工具
- `__init__.py`: 包初始化文件

### Strategy/ - 策略模块
- `MA.ipynb`: 移动平均策略实现

### Database/ - 数据库模块
- `history.db`: SQLite数据库文件，存储历史数据

### Config/ - 配置模块
- `config.yaml`: 项目配置文件

### Unittest/ - 单元测试模块
- `temptest.py`: 测试文件

### Notebooks/ - Jupyter笔记本目录
用于开发和测试策略的Jupyter笔记本

## 主要功能模块

### 数据获取模块
- 通过`data_fetcher.py`和`tushare_loader.py`实现数据获取功能
- 支持从Tushare获取股票数据

### 数据库管理
- 使用SQLite数据库存储历史数据
- 通过`db_manager.py`提供数据库操作接口

### 策略实现
- 在Strategy目录下实现交易策略
- 目前包含移动平均策略(MA)

### 配置管理
- 使用YAML格式的配置文件管理项目参数

## 开发环境
- Python项目
- 使用Jupyter Notebook进行策略开发和测试
- 包含单元测试框架