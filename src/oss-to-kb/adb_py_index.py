import json
import logging
import os
import urllib.parse
from typing import Dict, Any, Optional
from io import BytesIO
import oss2
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_gpdb20160503.client import Client as GPDBClient
from alibabacloud_gpdb20160503 import models as gpdb_models
from alibabacloud_tea_util import models as util_models

# 配置日志
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_json_dumps(obj, **kwargs):
    """安全的JSON序列化，处理bytes和其他不可序列化的类型"""
    def convert_item(item):
        if isinstance(item, bytes):
            try:
                return item.decode('utf-8')
            except:
                return f"<bytes: {len(item)} bytes>"
        elif isinstance(item, dict):
            return {k: convert_item(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [convert_item(i) for i in item]
        elif hasattr(item, '__dict__'):
            return str(item)
        else:
            return item
    
    try:
        converted_obj = convert_item(obj)
        return json.dumps(converted_obj, **kwargs)
    except Exception as e:
        logger.warning(f"JSON序列化失败: {str(e)}")
        return str(obj)


def decode_event_if_needed(event):
    """如果事件是编码的，则解码它"""
    try:
        if isinstance(event, bytes):
            event = event.decode('utf-8')
        
        if isinstance(event, str):
            try:
                import base64
                decoded_bytes = base64.b64decode(event)
                decoded_str = decoded_bytes.decode('utf-8')
                return json.loads(decoded_str)
            except:
                try:
                    return json.loads(event)
                except:
                    return event
        
        return event
        
    except Exception as e:
        logger.warning(f"解码事件失败: {str(e)}")
        return event


class OSSClient:
    """OSS客户端，用于下载文件内容"""
    
    def __init__(self):
        # 从函数计算环境获取阿里云凭证
        self.access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
        self.access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
        self.security_token = os.getenv('ALIBABA_CLOUD_SECURITY_TOKEN')
        
        self.region_id = os.getenv('GPDB_REGION_ID', 'cn-hangzhou')
        
        logger.info(f"初始化OSS客户端 - 区域: {self.region_id}")
        
    def get_bucket_client(self, bucket_name: str, region: str = None):
        """获取OSS Bucket客户端"""
        if not region:
            region = self.region_id
            
        endpoint = f'https://oss-{region}.aliyuncs.com'
        
        if self.security_token:
            auth = oss2.StsAuth(self.access_key_id, self.access_key_secret, self.security_token)
        else:
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        
        return oss2.Bucket(auth, endpoint, bucket_name)
    
    def download_file_content(self, bucket_name: str, object_key: str, region: str = None) -> bytes:
        """下载OSS文件内容"""
        try:
            bucket = self.get_bucket_client(bucket_name, region)
            
            # 下载文件内容
            obj = bucket.get_object(object_key)
            content = obj.read()
            
            logger.info(f"成功下载文件: {object_key}, 大小: {len(content)} 字节")
            return content
            
        except Exception as e:
            logger.error(f"下载文件失败 {bucket_name}/{object_key}: {str(e)}")
            raise


class AnalyticDBClient:
    """AnalyticDB PostgreSQL RAG Service客户端 - 使用官方SDK"""
    
    def __init__(self):
        # 从环境变量获取配置
        self.instance_id = os.getenv('GPDB_INSTANCE_ID')
        self.region_id = os.getenv('GPDB_REGION_ID', 'cn-hangzhou')
        self.collection = os.getenv('GPDB_COLLECTION', 'document')
        self.namespace = os.getenv('GPDB_NAMESPACE', 'public')
        self.namespace_password = os.getenv('GPDB_NAMESPACE_PASSWORD')
        
        # OSS触发器相关配置
        self.trigger_bucket = os.getenv('OSS_TRIGGER_BUCKET')
        self.prefix_filter = os.getenv('OSS_PREFIX_FILTER', 'wiki/')
        
        # 从函数计算环境获取阿里云凭证
        self.access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
        self.access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
        self.security_token = os.getenv('ALIBABA_CLOUD_SECURITY_TOKEN')
        
        logger.info(f"初始化AnalyticDB客户端")
        logger.info(f"  实例ID: {self.instance_id}")
        logger.info(f"  区域: {self.region_id}")
        logger.info(f"  文档集合: {self.collection}")
        logger.info(f"  命名空间: {self.namespace}")
        logger.info(f"  OSS触发器Bucket: {self.trigger_bucket}")
        logger.info(f"  前缀过滤: {self.prefix_filter}")
        
        # 验证配置
        self._validate_config()
        
        # 创建官方SDK客户端
        self.client = self._create_client()
    
    def _validate_config(self):
        """验证必要的配置参数"""
        required_configs = {
            'GPDB_INSTANCE_ID': self.instance_id,
            'GPDB_REGION_ID': self.region_id,
            'GPDB_NAMESPACE_PASSWORD': self.namespace_password,
            'ALIBABA_CLOUD_ACCESS_KEY_ID': self.access_key_id,
            'ALIBABA_CLOUD_ACCESS_KEY_SECRET': self.access_key_secret
        }
        
        missing = [key for key, value in required_configs.items() if not value]
        
        if missing:
            raise ValueError(f"缺少必要的环境变量: {', '.join(missing)}")
    
    def _create_client(self) -> GPDBClient:
        """创建官方SDK客户端"""
        config = open_api_models.Config(
            access_key_id=self.access_key_id,
            access_key_secret=self.access_key_secret
        )
        
        # 如果有 STS Token，添加到配置中
        if self.security_token:
            config.security_token = self.security_token
            logger.info("使用STS临时凭证")
        
        # 设置区域ID
        config.region_id = self.region_id
        
        # 根据官方文档设置endpoint
        if self.region_id in ("cn-beijing", "cn-hangzhou", "cn-shanghai", 
                             "cn-shenzhen", "cn-hongkong", "ap-southeast-1"):
            config.endpoint = "gpdb.aliyuncs.com"
        else:
            config.endpoint = f'gpdb.{self.region_id}.aliyuncs.com'
        
        logger.info(f"  API端点: {config.endpoint}")
        
        return GPDBClient(config)
    
    def upload_document_with_content(self, file_content: bytes, file_name: str, 
                                    metadata: Optional[Dict] = None) -> str:
        """直接上传文档内容到AnalyticDB"""
        try:
            # 将 bytes 转换为类文件对象
            file_obj = BytesIO(file_content)
            
            # 创建上传请求
            request = gpdb_models.UploadDocumentAsyncAdvanceRequest(
                region_id=self.region_id,
                dbinstance_id=self.instance_id,
                collection=self.collection,
                namespace=self.namespace,
                namespace_password=self.namespace_password,
                file_name=file_name,
                file_url_object=file_obj,
                chunk_size=500,
                chunk_overlap=50,
            )
            
            if metadata:
                request.metadata = json.dumps(metadata, ensure_ascii=False)
                logger.info(f"文档元数据: {metadata}")
            
            logger.info(f"开始上传文档: {file_name}")
            logger.info(f"  实例ID: {self.instance_id}")
            logger.info(f"  区域ID: {self.region_id}")
            logger.info(f"  文件大小: {len(file_content)} 字节")
            logger.info(f"  分块大小: 500")
            logger.info(f"  分块重叠: 50")
            
            # 创建 runtime 配置
            runtime = util_models.RuntimeOptions()
            runtime.read_timeout = 60000  # 60秒超时
            runtime.connect_timeout = 10000  # 10秒连接超时
            
            # 调用SDK上传
            response = self.client.upload_document_async_advance(request, runtime)
            
            logger.info(f"上传响应状态: {response.body.status}")
            
            if response.body.status == 'success':
                job_id = response.body.job_id
                logger.info(f"✓ 文档上传任务已提交")
                logger.info(f"  JobId: {job_id}")
                logger.info(f"  文件名: {file_name}")
                return job_id
            else:
                message = getattr(response.body, 'message', 'Unknown error')
                raise Exception(f"上传任务提交失败: {message}")
                
        except Exception as e:
            logger.error(f"✗ 上传文档失败")
            logger.error(f"  文件: {file_name}")
            logger.error(f"  错误: {str(e)}")
            raise
    
    def delete_document(self, file_name: str) -> bool:
        """删除AnalyticDB中的文档"""
        try:
            logger.info(f"开始删除文档: {file_name}")
            
            request = gpdb_models.DeleteDocumentRequest(
                region_id=self.region_id,
                dbinstance_id=self.instance_id,
                collection=self.collection,
                namespace=self.namespace,
                namespace_password=self.namespace_password,
                file_name=file_name
            )
            
            response = self.client.delete_document(request)
            
            if response.body.status == 'success':
                logger.info(f"✓ 文档删除成功: {file_name}")
                return True
            else:
                logger.error(f"✗ 文档删除失败: {file_name}, 状态: {response.body.status}")
                return False
                
        except Exception as e:
            logger.error(f"✗ 删除文档异常: {file_name}, 错误: {str(e)}")
            return False


class OSSEventProcessor:
    """OSS事件处理器"""
    
    # 需要忽略的文件名列表（完全匹配）
    IGNORED_FILES = {
        'sync_records.json',
        '.DS_Store',
        'Thumbs.db',
        '.gitkeep',
        '.gitignore'
    }
    
    # 需要忽略的文件名模式（前缀或后缀）
    IGNORED_PATTERNS = {
        'prefixes': ['.', '~', '_tmp_', 'temp_'],  # 以这些开头的文件
        'suffixes': ['.tmp', '.bak', '.swp', '~']   # 以这些结尾的文件
    }
    
    def __init__(self):
        self.adb_client = AnalyticDBClient()
        self.oss_client = OSSClient()
        
    def _extract_file_info_from_event(self, event: Dict[str, Any]) -> Dict[str, str]:
        """从OSS事件中提取文件信息"""
        try:
            records = event.get('events', [])
            if not records:
                raise ValueError("事件中没有找到events")
            
            record = records[0]
            oss_info = record.get('oss', {})
            
            bucket_name = oss_info.get('bucket', {}).get('name', '')
            object_key = urllib.parse.unquote(oss_info.get('object', {}).get('key', ''))
            event_name = record.get('eventName', '')
            
            # 从事件中获取区域信息
            region = record.get('region', 'oss-cn-hangzhou')
            if region.startswith('oss-'):
                region = region[4:]  # 移除 'oss-' 前缀
            
            file_info = {
                'bucket_name': bucket_name,
                'object_key': object_key,
                'file_name': object_key.split('/')[-1],
                'event_name': event_name,
                'region': region
            }
            
            logger.info(f"提取文件信息:")
            logger.info(f"  Bucket: {bucket_name}")
            logger.info(f"  对象键: {object_key}")
            logger.info(f"  文件名: {file_info['file_name']}")
            logger.info(f"  事件类型: {event_name}")
            logger.info(f"  区域: {region}")
            
            return file_info
            
        except Exception as e:
            logger.error(f"解析OSS事件失败: {str(e)}")
            raise
    
    def _is_ignored_file(self, file_name: str) -> tuple[bool, str]:
        """
        检查文件是否应该被忽略
        返回: (是否忽略, 忽略原因)
        """
        # 检查完全匹配的忽略文件
        if file_name in self.IGNORED_FILES:
            return True, f"文件在忽略列表中: {file_name}"
        
        # 检查文件名模式（前缀）
        for prefix in self.IGNORED_PATTERNS['prefixes']:
            if file_name.startswith(prefix):
                return True, f"文件名以忽略前缀开头: {prefix}"
        
        # 检查文件名模式（后缀）
        for suffix in self.IGNORED_PATTERNS['suffixes']:
            if file_name.endswith(suffix):
                return True, f"文件名以忽略后缀结尾: {suffix}"
        
        return False, ""
    
    def _should_process_file(self, object_key: str) -> tuple[bool, str]:
        """
        判断是否应该处理该文件
        返回: (是否处理, 原因说明)
        """
        file_name = object_key.split('/')[-1]
        
        # 1. 检查是否是忽略的文件
        is_ignored, ignore_reason = self._is_ignored_file(file_name)
        if is_ignored:
            logger.info(f"⊗ 跳过: {ignore_reason}")
            return False, ignore_reason
        
        # 2. 检查前缀过滤
        if not object_key.startswith(self.adb_client.prefix_filter):
            reason = f"文件不在处理范围内 (要求前缀: {self.adb_client.prefix_filter})"
            logger.info(f"⊗ 跳过: {reason}")
            return False, reason
        
        # 3. 检查是否是目录（以 / 结尾）
        if object_key.endswith('/'):
            reason = "对象是目录，不是文件"
            logger.info(f"⊗ 跳过: {reason}")
            return False, reason
        
        # 4. 支持的文件扩展名
        supported_extensions = [
            '.md', '.txt', '.pdf', '.docx', '.doc', 
            '.html', '.htm', '.json', '.csv', 
            '.py', '.java', '.cpp', '.c', '.h',
            '.js', '.ts', '.jsx', '.tsx',
            '.go', '.rs', '.rb', '.php',
            '.xml', '.yaml', '.yml', '.toml',
            '.sh', '.bash', '.sql'
        ]
        
        file_ext = os.path.splitext(object_key.lower())[1]
        
        if not file_ext:
            reason = "文件没有扩展名"
            logger.info(f"⊗ 跳过: {reason}")
            return False, reason
        
        if file_ext not in supported_extensions:
            reason = f"不支持的文件类型: {file_ext}"
            logger.info(f"⊗ 跳过: {reason}")
            return False, reason
        
        logger.info(f"✓ 文件符合处理条件")
        return True, "文件符合处理条件"
    
    def _extract_metadata_from_path(self, object_key: str) -> Dict[str, str]:
        """从文件路径中提取元数据"""
        import time
        
        path_parts = object_key.split('/')
        
        metadata = {
            'source': 'feishu_wiki',
            'sync_timestamp': str(int(time.time())),
            'full_path': object_key
        }
        
        # 提取空间名称（假设路径格式为: wiki/space_name/...）
        if len(path_parts) >= 2:
            metadata['space'] = path_parts[1]
        
        # 提取标题（去除扩展名）
        if len(path_parts) >= 1:
            metadata['title'] = os.path.splitext(path_parts[-1])[0]
        
        # 提取文件扩展名
        file_ext = os.path.splitext(object_key)[1]
        if file_ext:
            metadata['file_type'] = file_ext.lstrip('.')
        
        # 提取目录层级信息
        if len(path_parts) > 2:
            metadata['directories'] = '/'.join(path_parts[1:-1])
        
        logger.info(f"提取元数据: {metadata}")
        
        return metadata
    
    def process_create_event(self, file_info: Dict[str, str]) -> Dict[str, Any]:
        """处理文件创建事件"""
        logger.info(f"=" * 60)
        logger.info(f"处理文件创建事件")
        logger.info(f"=" * 60)
        
        should_process, reason = self._should_process_file(file_info['object_key'])
        if not should_process:
            return {
                'action': 'skip',
                'reason': reason,
                'file_name': file_info['file_name']
            }
        
        try:
            # 从OSS下载文件内容
            logger.info(f"步骤1: 从OSS下载文件")
            file_content = self.oss_client.download_file_content(
                bucket_name=file_info['bucket_name'],
                object_key=file_info['object_key'],
                region=file_info['region']
            )
            
            # 检查文件大小限制（200MB）
            max_size = 200 * 1024 * 1024
            if len(file_content) > max_size:
                raise Exception(f"文件太大: {len(file_content)} 字节，超过200MB限制")
            
            # 提取元数据
            metadata = self._extract_metadata_from_path(file_info['object_key'])
            metadata['event_type'] = 'create'
            
            # 上传到AnalyticDB
            logger.info(f"步骤2: 上传到AnalyticDB")
            job_id = self.adb_client.upload_document_with_content(
                file_content=file_content,
                file_name=file_info['file_name'],
                metadata=metadata
            )
            
            result = {
                'action': 'upload',
                'status': 'success',
                'job_id': job_id,
                'file_name': file_info['file_name'],
                'file_size': len(file_content),
                'metadata': metadata
            }
            
            logger.info(f"✓ 创建事件处理完成")
            return result
            
        except Exception as e:
            logger.error(f"✗ 处理文件创建事件失败: {str(e)}")
            return {
                'action': 'upload',
                'status': 'failed',
                'error': str(e),
                'file_name': file_info['file_name']
            }
    
    def process_update_event(self, file_info: Dict[str, str]) -> Dict[str, Any]:
        """处理文件更新事件（先删除再创建）"""
        logger.info(f"=" * 60)
        logger.info(f"处理文件更新事件")
        logger.info(f"=" * 60)
        
        should_process, reason = self._should_process_file(file_info['object_key'])
        if not should_process:
            return {
                'action': 'skip',
                'reason': reason,
                'file_name': file_info['file_name']
            }
        
        try:
            # 步骤1：删除旧文档
            logger.info(f"步骤1: 删除旧文档")
            delete_success = self.adb_client.delete_document(file_info['file_name'])
            
            # 步骤2：下载新文档
            logger.info(f"步骤2: 从OSS下载新文件")
            file_content = self.oss_client.download_file_content(
                bucket_name=file_info['bucket_name'],
                object_key=file_info['object_key'],
                region=file_info['region']
            )
            
            # 检查文件大小
            max_size = 200 * 1024 * 1024
            if len(file_content) > max_size:
                raise Exception(f"文件太大: {len(file_content)} 字节，超过200MB限制")
            
            # 提取元数据
            metadata = self._extract_metadata_from_path(file_info['object_key'])
            metadata['event_type'] = 'update'
            
            # 步骤3：上传新文档
            logger.info(f"步骤3: 上传新文档到AnalyticDB")
            job_id = self.adb_client.upload_document_with_content(
                file_content=file_content,
                file_name=file_info['file_name'],
                metadata=metadata
            )
            
            result = {
                'action': 'update',
                'status': 'success',
                'delete_success': delete_success,
                'upload_job_id': job_id,
                'file_name': file_info['file_name'],
                'file_size': len(file_content),
                'metadata': metadata
            }
            
            logger.info(f"✓ 更新事件处理完成")
            return result
            
        except Exception as e:
            logger.error(f"✗ 处理文件更新事件失败: {str(e)}")
            return {
                'action': 'update',
                'status': 'failed',
                'error': str(e),
                'file_name': file_info['file_name']
            }
    
    def process_delete_event(self, file_info: Dict[str, str]) -> Dict[str, Any]:
        """处理文件删除事件"""
        logger.info(f"=" * 60)
        logger.info(f"处理文件删除事件")
        logger.info(f"=" * 60)
        
        should_process, reason = self._should_process_file(file_info['object_key'])
        if not should_process:
            return {
                'action': 'skip',
                'reason': reason,
                'file_name': file_info['file_name']
            }
        
        try:
            success = self.adb_client.delete_document(file_info['file_name'])
            
            result = {
                'action': 'delete',
                'status': 'success' if success else 'failed',
                'file_name': file_info['file_name']
            }
            
            logger.info(f"✓ 删除事件处理完成")
            return result
            
        except Exception as e:
            logger.error(f"✗ 处理文件删除事件失败: {str(e)}")
            return {
                'action': 'delete',
                'status': 'failed',
                'error': str(e),
                'file_name': file_info['file_name']
            }


def handler(event, context):
    """函数计算OSS触发器入口点"""
    logger.info("=" * 80)
    logger.info("开始处理OSS触发事件")
    logger.info("=" * 80)
    logger.info(f"请求ID: {context.request_id}")
    
    try:
        # 解码事件
        decoded_event = decode_event_if_needed(event)
        logger.info(f"事件类型: {type(decoded_event)}")
        
        try:
            event_str = safe_json_dumps(decoded_event, indent=2, ensure_ascii=False)
            logger.info(f"事件内容:\n{event_str}")
        except Exception as e:
            logger.warning(f"无法序列化事件内容: {str(e)}")
        
        # 创建处理器
        processor = OSSEventProcessor()
        
        # 提取文件信息
        file_info = processor._extract_file_info_from_event(decoded_event)
        event_name = file_info['event_name']
        
        # 根据事件类型处理
        if 'ObjectCreated' in event_name:
            result = processor.process_create_event(file_info)
        elif 'ObjectModified' in event_name or 'ObjectOverwrote' in event_name:
            result = processor.process_update_event(file_info)
        elif 'ObjectRemoved' in event_name:
            result = processor.process_delete_event(file_info)
        else:
            logger.warning(f"不支持的事件类型: {event_name}")
            result = {'action': 'unsupported', 'event_name': event_name}
        
        # 构造响应
        response = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'success': True,
                'message': 'OSS事件处理完成',
                'request_id': context.request_id,
                'event_name': event_name,
                'file_info': file_info,
                'result': result
            }, ensure_ascii=False)
        }
        
        logger.info(f"=" * 80)
        logger.info(f"✓ 事件处理完成")
        logger.info(f"  处理结果: {json.dumps(result, ensure_ascii=False)}")
        logger.info(f"=" * 80)
        
        return response
        
    except Exception as e:
        error_response = {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'success': False,
                'message': f'OSS事件处理失败: {str(e)}',
                'request_id': context.request_id,
                'error': str(e)
            }, ensure_ascii=False)
        }
        
        logger.error(f"=" * 80)
        logger.error(f"✗ OSS事件处理失败")
        logger.error(f"  错误: {str(e)}")
        logger.error(f"=" * 80)
        logger.error("详细错误信息:", exc_info=True)
        
        return error_response
