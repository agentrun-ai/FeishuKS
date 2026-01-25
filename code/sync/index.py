import os
import json
import logging
import hashlib
import tempfile
import shutil
import time
from pathlib import Path
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
import requests
import oss2
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# 配置日志 - 函数计算环境优化
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class WikiNode:
    """知识库节点数据结构"""
    node_token: str
    obj_token: str
    obj_type: str
    title: str
    space_id: str
    obj_edit_time: str  # 新增编辑时间字段
    parent_node_token: Optional[str] = None
    has_child: bool = False
    node_create_time: Optional[str] = None
    obj_create_time: Optional[str] = None

@dataclass
class SyncRecord:
    """同步记录数据结构"""
    obj_token: str
    title: str
    oss_path: str
    content_hash: str
    last_sync: int
    obj_edit_time: str  # 新增编辑时间字段
    obj_type: str = 'docx'

class RetryableError(Exception):
    """可重试的错误"""
    pass

class RateLimitError(RetryableError):
    """限流错误"""
    pass

class FeishuWikiSyncer:
    """飞书知识库同步器"""
    
    def __init__(self, context=None):
        # 从环境变量获取配置
        self.app_id = os.getenv('FEISHU_APP_ID')
        self.app_secret = os.getenv('FEISHU_APP_SECRET')
        self.space_name = os.getenv('WIKI_SPACE_NAME')  # 可选，知识库名称
        self.space_id = os.getenv('WIKI_SPACE_ID')      # 可选，直接指定space_id
        
        # OSS配置
        self.oss_endpoint = os.getenv('OSS_ENDPOINT')
        self.oss_bucket_name = os.getenv('OSS_BUCKET_NAME')
        self.oss_prefix = os.getenv('OSS_PREFIX', 'wiki/')
        
        # 重试配置
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay_base = float(os.getenv('RETRY_DELAY_BASE', '1.0'))
        
        # 从函数计算系统环境变量获取阿里云凭证
        self.oss_access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
        self.oss_access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
        self.oss_security_token = os.getenv('ALIBABA_CLOUD_SECURITY_TOKEN')
        
        # 本地存储配置
        self.local_storage_path = os.getenv('LOCAL_STORAGE_PATH', '/tmp/wiki_docs')
        self.sync_record_file = os.path.join(self.local_storage_path, 'sync_records.json')
        
        logger.info(f"使用函数计算系统环境变量凭证")
        logger.info(f"Access Key ID exists: {bool(self.oss_access_key_id)}")
        logger.info(f"Security Token exists: {bool(self.oss_security_token)}")
        logger.info(f"本地存储路径: {self.local_storage_path}")
        logger.info(f"重试配置: 最大重试次数={self.max_retries}, 基础延迟={self.retry_delay_base}s")
        
        # 飞书API配置
        self.base_url = 'https://open.feishu.cn/open-apis'
        self.access_token = None
        
        # 同步记录
        self.sync_records: Dict[str, SyncRecord] = {}
        
        # 初始化本地存储目录
        self._init_local_storage()
        
        # 初始化OSS客户端
        self._init_oss_client()
        
        # 加载同步记录
        self._load_sync_records()
        
        # 参数校验
        self._validate_config()
    
    def _init_local_storage(self):
        """初始化本地存储目录"""
        try:
            os.makedirs(self.local_storage_path, exist_ok=True)
            logger.info(f"本地存储目录初始化成功: {self.local_storage_path}")
        except Exception as e:
            logger.error(f"本地存储目录初始化失败: {str(e)}")
            raise
    
    def _init_oss_client(self):
        """初始化OSS客户端"""
        try:
            if self.oss_security_token:
                # 使用STS临时凭证（函数计算提供）
                auth = oss2.StsAuth(
                    self.oss_access_key_id, 
                    self.oss_access_key_secret, 
                    self.oss_security_token
                )
                logger.info("使用STS临时凭证初始化OSS客户端")
            else:
                # 使用普通密钥（备用）
                auth = oss2.Auth(self.oss_access_key_id, self.oss_access_key_secret)
                logger.info("使用普通密钥初始化OSS客户端")
            
            self.oss_bucket = oss2.Bucket(auth, self.oss_endpoint, self.oss_bucket_name)
            logger.info("OSS客户端初始化成功")
            
        except Exception as e:
            logger.error(f"OSS客户端初始化失败: {str(e)}")
            raise
    
    def _load_sync_records(self):
        """从OSS加载同步记录"""
        try:
            # 尝试从OSS下载同步记录
            oss_record_path = f"{self.oss_prefix}sync_records.json"
            
            try:
                result = self.oss_bucket.get_object(oss_record_path)
                record_data = result.read().decode('utf-8')
                records_dict = json.loads(record_data)
                
                # 转换为SyncRecord对象
                for obj_token, record_dict in records_dict.items():
                    self.sync_records[obj_token] = SyncRecord(
                        obj_token=record_dict['obj_token'],
                        title=record_dict['title'],
                        oss_path=record_dict['oss_path'],
                        content_hash=record_dict['content_hash'],
                        last_sync=record_dict['last_sync'],
                        obj_edit_time=record_dict.get('obj_edit_time', ''),
                        obj_type=record_dict.get('obj_type', 'docx')
                    )
                
                logger.info(f"成功从OSS加载 {len(self.sync_records)} 条同步记录")
                
            except oss2.exceptions.NoSuchKey:
                logger.info("OSS中没有找到同步记录文件，将创建新的记录")
            except Exception as e:
                logger.warning(f"从OSS加载同步记录失败: {str(e)}")
                
        except Exception as e:
            logger.error(f"初始化同步记录失败: {str(e)}")
    
    def _save_sync_records(self):
        """保存同步记录到OSS"""
        try:
            # 转换为可序列化的字典
            records_dict = {}
            for obj_token, record in self.sync_records.items():
                records_dict[obj_token] = {
                    'obj_token': record.obj_token,
                    'title': record.title,
                    'oss_path': record.oss_path,
                    'content_hash': record.content_hash,
                    'last_sync': record.last_sync,
                    'obj_edit_time': record.obj_edit_time,
                    'obj_type': record.obj_type
                }
            
            # 保存到本地
            with open(self.sync_record_file, 'w', encoding='utf-8') as f:
                json.dump(records_dict, f, ensure_ascii=False, indent=2)
            
            # 上传到OSS（带重试）
            oss_record_path = f"{self.oss_prefix}sync_records.json"
            self._retry_upload_file(self.sync_record_file, oss_record_path)
            
            logger.info(f"成功保存 {len(self.sync_records)} 条同步记录到OSS")
            
        except Exception as e:
            logger.error(f"保存同步记录失败: {str(e)}")
    
    def _validate_config(self):
        """验证必要的配置参数"""
        required_feishu_configs = [self.app_id, self.app_secret]
        required_oss_configs = [self.oss_endpoint, self.oss_access_key_id, 
                               self.oss_access_key_secret, self.oss_bucket_name]
        
        if not all(required_feishu_configs):
            raise ValueError("飞书配置参数不完整，请检查FEISHU_APP_ID和FEISHU_APP_SECRET环境变量")
        
        if not all(required_oss_configs):
            missing_configs = []
            if not self.oss_endpoint:
                missing_configs.append("OSS_ENDPOINT")
            if not self.oss_access_key_id:
                missing_configs.append("ALIBABA_CLOUD_ACCESS_KEY_ID")
            if not self.oss_access_key_secret:
                missing_configs.append("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
            if not self.oss_bucket_name:
                missing_configs.append("OSS_BUCKET_NAME")
            
            raise ValueError(f"OSS配置参数不完整，缺少环境变量: {', '.join(missing_configs)}")
            
        if not (self.space_name or self.space_id):
            raise ValueError("必须提供WIKI_SPACE_NAME或WIKI_SPACE_ID之一")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """带指数退避的重试装饰器"""
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except RateLimitError as e:
                if attempt == self.max_retries:
                    raise
                delay = self.retry_delay_base * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"遇到限流，等待 {delay:.2f}s 后重试 (尝试 {attempt + 1}/{self.max_retries + 1})")
                time.sleep(delay)
            except RetryableError as e:
                if attempt == self.max_retries:
                    raise
                delay = self.retry_delay_base * (2 ** attempt)
                logger.warning(f"遇到可重试错误: {str(e)}, 等待 {delay:.2f}s 后重试 (尝试 {attempt + 1}/{self.max_retries + 1})")
                time.sleep(delay)
            except Exception as e:
                # 不可重试的错误直接抛出
                raise
    
    def get_tenant_access_token(self) -> str:
        """获取tenant_access_token"""
        def _get_token():
            url = f'{self.base_url}/auth/v3/tenant_access_token/internal'
            headers = {'Content-Type': 'application/json; charset=utf-8'}
            data = {
                'app_id': self.app_id,
                'app_secret': self.app_secret
            }
            
            response = requests.post(url, headers=headers, json=data, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            if result['code'] != 0:
                raise Exception(f"获取access_token失败: {result['msg']}")
                
            self.access_token = result['tenant_access_token']
            logger.info("成功获取tenant_access_token")
            return self.access_token
        
        return self._retry_with_backoff(_get_token)
    
    def get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        if not self.access_token:
            self.get_tenant_access_token()
        
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json; charset=utf-8'
        }
    
    def _handle_api_error(self, response):
        """处理API错误响应"""
        try:
            error_data = response.json()
            error_code = error_data.get('code', 'unknown')
            error_msg = error_data.get('msg', 'unknown error')
            
            # 检查是否是限流错误
            if error_code in [131001, 131007] or response.status_code == 429:
                raise RateLimitError(f"API限流 ({error_code}): {error_msg}")
            
            logger.error(f"飞书API错误 - Code: {error_code}, Message: {error_msg}")
            return f"飞书API错误 ({error_code}): {error_msg}"
        except RateLimitError:
            raise
        except:
            logger.error(f"HTTP错误: {response.status_code} - {response.text}")
            if response.status_code == 429:
                raise RateLimitError(f"HTTP限流: {response.status_code}")
            return f"HTTP错误: {response.status_code}"
    
    def get_wiki_spaces(self) -> List[Dict]:
        """获取知识空间列表"""
        def _get_spaces():
            url = f'{self.base_url}/wiki/v2/spaces'
            headers = self.get_headers()
            
            all_spaces = []
            page_token = None
            
            while True:
                params = {'page_size': 50}
                if page_token:
                    params['page_token'] = page_token
                    
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if not response.ok:
                    error_msg = self._handle_api_error(response)
                    raise Exception(error_msg)
                
                result = response.json()
                if result['code'] != 0:
                    raise Exception(f"获取知识空间列表失败: {result['msg']}")
                
                data = result['data']
                all_spaces.extend(data['items'])
                
                if not data.get('has_more', False):
                    break
                page_token = data.get('page_token')
            
            return all_spaces
        
        spaces = self._retry_with_backoff(_get_spaces)
        logger.info(f"找到 {len(spaces)} 个知识空间")
        return spaces
    
    def find_space_id(self) -> str:
        """根据空间名称查找space_id"""
        if self.space_id:
            logger.info(f"直接使用提供的space_id: {self.space_id}")
            return self.space_id
        
        spaces = self.get_wiki_spaces()
        logger.info(f"搜索知识空间 '{self.space_name}'，在 {len(spaces)} 个空间中查找")
        
        for space in spaces:
            logger.info(f"检查空间: {space.get('name', '未知名称')}")
            if space['name'] == self.space_name:
                found_space_id = space['space_id']
                logger.info(f"找到知识空间 '{self.space_name}': {found_space_id}")
                return found_space_id
        
        # 列出所有可用空间名称帮助调试
        available_spaces = [space.get('name', '未知名称') for space in spaces]
        logger.error(f"可用的知识空间列表: {available_spaces}")
        raise Exception(f"未找到名为 '{self.space_name}' 的知识空间")
    
    def get_wiki_nodes(self, space_id: str, parent_node_token: Optional[str] = None) -> List[WikiNode]:
        """获取知识空间子节点列表（递归获取所有节点）"""
        def _get_nodes():
            url = f'{self.base_url}/wiki/v2/spaces/{space_id}/nodes'
            headers = self.get_headers()
            
            all_nodes = []
            page_token = None
            
            while True:
                params = {'page_size': 50}
                if page_token:
                    params['page_token'] = page_token
                if parent_node_token:
                    params['parent_node_token'] = parent_node_token
                    
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if not response.ok:
                    error_msg = self._handle_api_error(response)
                    logger.warning(f"获取节点列表失败: {error_msg}")
                    break
                
                result = response.json()
                if result['code'] != 0:
                    logger.warning(f"获取节点列表失败: {result['msg']}")
                    break
                
                data = result['data']
                nodes = data['items']
                
                # 转换为WikiNode对象
                for node in nodes:
                    wiki_node = WikiNode(
                        node_token=node['node_token'],
                        obj_token=node['obj_token'],
                        obj_type=node['obj_type'],
                        title=node['title'],
                        space_id=space_id,
                        obj_edit_time=node.get('obj_edit_time', ''),
                        parent_node_token=parent_node_token,
                        has_child=node.get('has_child', False),
                        node_create_time=node.get('node_create_time'),
                        obj_create_time=node.get('obj_create_time')
                    )
                    all_nodes.append(wiki_node)
                    
                    # 如果节点有子节点，递归获取
                    if wiki_node.has_child:
                        try:
                            child_nodes = self.get_wiki_nodes(space_id, wiki_node.node_token)
                            all_nodes.extend(child_nodes)
                        except Exception as e:
                            logger.error(f"获取子节点失败 {wiki_node.title}: {str(e)}")
                
                if not data.get('has_more', False):
                    break
                page_token = data.get('page_token')
            
            return all_nodes
        
        nodes = self._retry_with_backoff(_get_nodes)
        logger.info(f"获取到 {len(nodes)} 个节点 (parent: {parent_node_token or 'root'})")
        return nodes
    
    def get_document_content(self, obj_token: str, obj_type: str) -> Optional[str]:
        """获取云文档内容"""
        if obj_type != 'docx':
            logger.debug(f"跳过非文档类型: {obj_type}")
            return None
        
        def _get_content():
            url = f'{self.base_url}/docs/v1/content'
            headers = self.get_headers()
            params = {
                'doc_token': obj_token,
                'doc_type': 'docx',
                'content_type': 'markdown',
                'lang': 'zh'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if not response.ok:
                error_msg = self._handle_api_error(response)
                raise RetryableError(f"获取文档内容失败 {obj_token}: {error_msg}")
            
            result = response.json()
            if result.get('code') != 0:
                error_msg = result.get('msg', '未知错误')
                if result.get('code') in [131001, 131007]:
                    raise RateLimitError(f"获取文档内容限流 {obj_token}: {error_msg}")
                raise Exception(f"获取文档内容失败 {obj_token}: {error_msg}")
            
            # 根据API响应结构获取content
            data = result.get('data', {})
            content = data.get('content', '')
            
            if not content:
                logger.warning(f"文档内容为空: {obj_token}")
                return None
            
            logger.info(f"成功获取文档内容: {obj_token}, 长度: {len(content)}")
            return content
        
        try:
            return self._retry_with_backoff(_get_content)
        except Exception as e:
            logger.error(f"获取文档内容异常 {obj_token}: {str(e)}")
            return None
    
    def generate_local_path(self, node: WikiNode, space_name: str = None) -> str:
        """生成本地文件路径（不包含token）"""
        # 使用空间名称或ID作为子目录
        root_dir = space_name or node.space_id
        safe_root_dir = self._sanitize_filename(root_dir)
        
        # 创建子目录
        local_dir = os.path.join(self.local_storage_path, safe_root_dir)
        os.makedirs(local_dir, exist_ok=True)
        
        # 构建文件路径（不包含token）
        safe_title = self._sanitize_filename(node.title)
        filename = f"{safe_title}.md"
        local_path = os.path.join(local_dir, filename)
        
        return local_path
    
    def generate_oss_path(self, node: WikiNode, space_name: str = None) -> str:
        """生成OSS存储路径（不包含token）"""
        # 使用空间名称或ID作为根目录
        root_dir = space_name or node.space_id
        safe_root_dir = self._sanitize_filename(root_dir)
        
        # 构建路径：prefix/space/title.md（不包含token）
        safe_title = self._sanitize_filename(node.title)
        file_path = f"{self.oss_prefix}{safe_root_dir}/{safe_title}.md"
        
        return file_path
    
    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名中的非法字符"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()
    
    def save_to_local(self, content: str, local_path: str, metadata: Dict = None) -> bool:
        """保存内容到本地文件"""
        try:
            # 创建文件目录
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # 写入文件内容
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # 如果有元数据，保存为同名的.json文件
            if metadata:
                metadata_path = local_path + '.meta.json'
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            logger.info(f"成功保存到本地: {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"保存本地文件失败 {local_path}: {str(e)}")
            return False
    
    def _retry_upload_file(self, local_path: str, oss_path: str) -> bool:
        """带重试的文件上传"""
        def _upload():
            # 检查本地文件是否存在
            if not os.path.exists(local_path):
                raise Exception(f"本地文件不存在: {local_path}")
            
            # 上传文件到OSS
            with open(local_path, 'rb') as f:
                result = self.oss_bucket.put_object(oss_path, f)
            
            return True
        
        try:
            self._retry_with_backoff(_upload)
            logger.info(f"成功上传到OSS: {local_path} -> {oss_path}")
            return True
        except Exception as e:
            logger.error(f"上传OSS失败 {local_path} -> {oss_path}: {str(e)}")
            return False
    
    def delete_from_oss(self, oss_path: str) -> bool:
        """从OSS删除文件"""
        def _delete():
            self.oss_bucket.delete_object(oss_path)
            return True
        
        try:
            self._retry_with_backoff(_delete)
            logger.info(f"成功从OSS删除文件: {oss_path}")
            return True
        except Exception as e:
            logger.error(f"从OSS删除文件失败 {oss_path}: {str(e)}")
            return False
    
    def need_update(self, node: WikiNode) -> bool:
        """检查文档是否需要更新（基于编辑时间）"""
        obj_token = node.obj_token
        
        # 如果没有同步记录，需要同步
        if obj_token not in self.sync_records:
            logger.info(f"新文档需要同步: {node.title}")
            return True
        
        record = self.sync_records[obj_token]
        
        # 检查文档标题是否变化
        if record.title != node.title:
            logger.info(f"文档标题已变化: {record.title} -> {node.title}")
            return True
        
        # 检查编辑时间是否变化（关键优化点）
        if record.obj_edit_time != node.obj_edit_time:
            logger.info(f"文档编辑时间已变化: {record.obj_edit_time} -> {node.obj_edit_time} ({node.title})")
            return True
        
        logger.debug(f"文档无变化，跳过同步: {node.title}")
        return False
    
    def handle_title_change(self, node: WikiNode, old_record: SyncRecord, space_name: str = None) -> bool:
        """处理文档标题变化（删除旧文件，创建新文件）"""
        try:
            # 删除旧的OSS文件
            if self.delete_from_oss(old_record.oss_path):
                logger.info(f"已删除旧文件: {old_record.oss_path}")
            
            # 生成新的路径
            new_oss_path = self.generate_oss_path(node, space_name)
            logger.info(f"文档重命名: {old_record.oss_path} -> {new_oss_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"处理标题变化失败 {node.title}: {str(e)}")
            return False
    
    def sync_single_document(self, node: WikiNode, space_name: str = None) -> bool:
        """同步单个文档"""
        try:
            # 1. 检查是否需要更新（基于编辑时间，避免不必要的API调用）
            if not self.need_update(node):
                return True  # 跳过，但算作成功
            
            # 2. 获取文档内容（只在需要时调用）
            content = self.get_document_content(node.obj_token, node.obj_type)
            if not content:
                return False
            
            # 3. 检查是否是标题变化
            old_record = self.sync_records.get(node.obj_token)
            if old_record and old_record.title != node.title:
                self.handle_title_change(node, old_record, space_name)
            
            # 4. 生成本地和OSS路径
            local_path = self.generate_local_path(node, space_name)
            oss_path = self.generate_oss_path(node, space_name)
            
            # 5. 准备元数据
            content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
            current_time = int(time.time())
            
            metadata = {
                'node_token': node.node_token,
                'obj_token': node.obj_token,
                'obj_type': node.obj_type,
                'title': node.title,
                'space_id': node.space_id,
                'obj_edit_time': node.obj_edit_time,
                'sync_timestamp': current_time,
                'content_hash': content_hash,
                'local_path': local_path,
                'oss_path': oss_path
            }
            
            # 6. 保存到本地
            if not self.save_to_local(content, local_path, metadata):
                return False
            
            # 7. 上传到OSS（带重试）
            if not self._retry_upload_file(local_path, oss_path):
                return False
            
            # 8. 更新同步记录
            self.sync_records[node.obj_token] = SyncRecord(
                obj_token=node.obj_token,
                title=node.title,
                oss_path=oss_path,
                content_hash=content_hash,
                last_sync=current_time,
                obj_edit_time=node.obj_edit_time,
                obj_type=node.obj_type
            )
            
            logger.info(f"文档同步成功: {node.title} -> {oss_path}")
            return True
            
        except Exception as e:
            logger.error(f"同步文档失败 {node.title}: {str(e)}")
            return False
    
    def handle_deleted_documents(self, current_docs: Set[str], space_name: str = None) -> int:
        """处理已删除的文档"""
        deleted_count = 0
        docs_to_remove = []
        
        for obj_token, record in self.sync_records.items():
            if obj_token not in current_docs:
                # 文档已被删除，从OSS中删除对应文件
                if self.delete_from_oss(record.oss_path):
                    logger.info(f"已删除文档: {record.title} ({record.oss_path})")
                    docs_to_remove.append(obj_token)
                    deleted_count += 1
        
        # 从同步记录中移除已删除的文档
        for obj_token in docs_to_remove:
            del self.sync_records[obj_token]
        
        if deleted_count > 0:
            logger.info(f"处理了 {deleted_count} 个已删除的文档")
        
        return deleted_count
    
    def sync_documents_parallel(self, nodes: List[WikiNode], space_name: str = None, max_workers: int = 2):
        """并发同步多个文档"""
        # 筛选出文档类型的节点
        doc_nodes = [node for node in nodes if node.obj_type == 'docx']
        
        logger.info(f"开始同步 {len(doc_nodes)} 个文档")
        
        # 先过滤出真正需要同步的文档
        nodes_to_sync = []
        skipped_count = 0
        
        for node in doc_nodes:
            if self.need_update(node):
                nodes_to_sync.append(node)
            else:
                skipped_count += 1
        
        logger.info(f"需要同步的文档: {len(nodes_to_sync)}, 跳过的文档: {skipped_count}")
        
        # 获取当前文档的obj_token集合，用于检测删除的文档
        current_doc_tokens = {node.obj_token for node in doc_nodes}
        
        successful = 0
        failed = 0
        
        # 降低并发数以避免限流
        if nodes_to_sync:
            logger.info(f"使用并发数: {max_workers}")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_node = {
                    executor.submit(self.sync_single_document, node, space_name): node 
                    for node in nodes_to_sync
                }
                
                # 处理结果
                for future in as_completed(future_to_node):
                    node = future_to_node[future]
                    try:
                        success = future.result()
                        if success:
                            successful += 1
                        else:
                            failed += 1
                    except Exception as e:
                        logger.error(f"同步文档失败 {node.title}: {str(e)}")
                        failed += 1
        
        # 处理已删除的文档
        deleted_count = self.handle_deleted_documents(current_doc_tokens, space_name)
        
        logger.info(f"同步完成 - 成功: {successful}, 失败: {failed}, 跳过: {skipped_count}, 删除: {deleted_count}")
        return {
            'successful': successful, 
            'failed': failed, 
            'skipped': skipped_count, 
            'deleted': deleted_count
        }
    
    def cleanup_local_files(self):
        """清理本地临时文件"""
        try:
            if os.path.exists(self.local_storage_path):
                shutil.rmtree(self.local_storage_path)
                logger.info(f"清理本地文件完成: {self.local_storage_path}")
        except Exception as e:
            logger.warning(f"清理本地文件失败: {str(e)}")
    
    def test_api_permissions(self):
        """测试API权限"""
        logger.info("开始测试飞书API权限")
        
        # 测试基本连接
        try:
            self.get_tenant_access_token()
            logger.info("✓ 获取access_token成功")
        except Exception as e:
            logger.error(f"✗ 获取access_token失败: {str(e)}")
            return False
        
        # 测试知识库权限
        try:
            spaces = self.get_wiki_spaces()
            logger.info(f"✓ 获取知识空间列表成功，共 {len(spaces)} 个空间")
            
            if spaces:
                logger.info("可用的知识空间:")
                for i, space in enumerate(spaces[:5]):  # 只显示前5个
                    logger.info(f"  {i+1}. {space.get('name', '未知名称')} (ID: {space.get('space_id', '未知ID')})")
                if len(spaces) > 5:
                    logger.info(f"  ... 还有 {len(spaces) - 5} 个空间")
                    
            return True
        except Exception as e:
            logger.error(f"✗ 获取知识空间列表失败: {str(e)}")
            return False
    
    def sync_wiki_space(self):
        """同步整个知识空间"""
        logger.info("开始同步飞书知识库到OSS")
        
        try:
            # 先测试权限
            if not self.test_api_permissions():
                return {
                    'code': -1, 
                    'message': 'API权限测试失败，请检查应用配置和权限设置'
                }
            
            # 1. 查找空间ID
            space_id = self.find_space_id()
            
            # 2. 获取所有节点（包含编辑时间信息）
            nodes = self.get_wiki_nodes(space_id)
            
            if not nodes:
                logger.warning("没有找到任何文档节点")
                return {'code': 1, 'message': '没有找到任何文档节点'}
            
            # 3. 同步文档（利用编辑时间优化）
            result = self.sync_documents_parallel(nodes, self.space_name)
            
            # 4. 保存同步记录
            self._save_sync_records()
            
            # 5. 生成同步报告
            sync_report = {
                'code': 0,
                'message': '同步完成',
                'space_id': space_id,
                'space_name': self.space_name,
                'total_nodes': len(nodes),
                'doc_nodes': len([n for n in nodes if n.obj_type == 'docx']),
                'successful': result['successful'],
                'failed': result['failed'],
                'skipped': result['skipped'],
                'deleted': result['deleted'],
                'oss_prefix': self.oss_prefix,
                'local_storage_path': self.local_storage_path,
                'sync_records_count': len(self.sync_records),
                'api_calls_saved': result['skipped']  # 通过跳过节省的API调用次数
            }
            
            logger.info(f"知识库同步完成: {json.dumps(sync_report, ensure_ascii=False)}")
            return sync_report
            
        except Exception as e:
            error_msg = f"知识库同步失败: {str(e)}"
            logger.error(error_msg)
            return {'code': -1, 'message': error_msg}

# 函数计算入口点
def handler(event, context):
    """函数计算主入口"""
    logger.info("开始执行飞书知识库同步任务")
    logger.info(f"请求ID: {context.request_id}")
    
    syncer = None
    try:
        # 初始化同步器
        syncer = FeishuWikiSyncer(context)
        
        # 执行同步
        result = syncer.sync_wiki_space()
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(result, ensure_ascii=False)
        }
        
    except Exception as e:
        error_response = {
            'code': -1,
            'message': f'执行失败: {str(e)}'
        }
        logger.error(f"函数执行失败: {str(e)}")
        
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(error_response, ensure_ascii=False)
        }
    
    finally:
        # 可选：清理本地文件
        if syncer:
            syncer.cleanup_local_files()
