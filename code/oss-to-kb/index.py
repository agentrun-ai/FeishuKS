import json
import logging
import os
import urllib.parse
from typing import Dict, Any, Optional
import requests
import hashlib
import hmac
import base64
from datetime import datetime

# 配置日志
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticDBClient:
    """AnalyticDB PostgreSQL RAG Service客户端"""
    
    def __init__(self):
        # 从环境变量获取配置
        self.instance_id = os.getenv('GPDB_INSTANCE_ID')
        self.region_id = os.getenv('GPDB_REGION_ID', 'cn-hangzhou')
        self.collection = os.getenv('GPDB_COLLECTION', 'document')
        self.namespace = os.getenv('GPDB_NAMESPACE', 'public')
        self.namespace_password = os.getenv('GPDB_NAMESPACE_PASSWORD')
        self.endpoint = os.getenv('GPDB_ENDPOINT', f'https://gpdb.{self.region_id}.aliyuncs.com')
        
        # OSS触发器相关配置
        self.trigger_bucket = os.getenv('OSS_TRIGGER_BUCKET')
        self.prefix_filter = os.getenv('OSS_PREFIX_FILTER', 'wiki/')
        
        # 从函数计算环境获取阿里云凭证
        self.access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
        self.access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
        self.security_token = os.getenv('ALIBABA_CLOUD_SECURITY_TOKEN')
        
        logger.info(f"初始化AnalyticDB客户端 - 实例: {self.instance_id}, 区域: {self.region_id}")
        
        # 验证配置
        self._validate_config()
    
    def _validate_config(self):
        """验证必要的配置参数"""
        required_configs = [
            self.instance_id, self.region_id, self.collection,
            self.namespace_password, self.access_key_id, self.access_key_secret
        ]
        
        if not all(required_configs):
            missing = []
            if not self.instance_id:
                missing.append("GPDB_INSTANCE_ID")
            if not self.namespace_password:
                missing.append("GPDB_NAMESPACE_PASSWORD")
            if not self.access_key_id:
                missing.append("ALIBABA_CLOUD_ACCESS_KEY_ID")
            if not self.access_key_secret:
                missing.append("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
                
            raise ValueError(f"缺少必要的环境变量: {', '.join(missing)}")
    
    def _generate_signature(self, method: str, url: str, params: Dict[str, str], body: str = "") -> str:
        """生成阿里云API签名"""
        # 构建签名字符串
        sorted_params = sorted(params.items())
        query_string = '&'.join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in sorted_params])
        
        # 解析URL
        parsed_url = urllib.parse.urlparse(url)
        canonical_uri = parsed_url.path or '/'
        
        string_to_sign = f"{method.upper()}\n{parsed_url.netloc}\n{canonical_uri}\n{query_string}"
        
        if body:
            string_to_sign += f"\n{body}"
        
        # 使用HMAC-SHA256生成签名
        signature = base64.b64encode(
            hmac.new(
                self.access_key_secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        
        return signature
    
    def _make_request(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """发送API请求到AnalyticDB"""
        # 公共参数
        common_params = {
            'Action': action,
            'RegionId': self.region_id,
            'AccessKeyId': self.access_key_id,
            'SignatureMethod': 'HMAC-SHA256',
            'SignatureVersion': '1.0',
            'SignatureNonce': str(hash(datetime.now())),
            'Timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'Version': '2016-05-03',
            'Format': 'JSON'
        }
        
        # 添加STS Token（如果存在）
        if self.security_token:
            common_params['SecurityToken'] = self.security_token
        
        # 合并参数
        all_params = {**common_params, **params}
        
        # 生成签名
        signature = self._generate_signature('POST', self.endpoint, all_params)
        all_params['Signature'] = signature
        
        # 发送请求
        try:
            response = requests.post(
                self.endpoint,
                data=all_params,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get('Code'):
                raise Exception(f"API调用失败: {result.get('Message', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"API请求失败 - Action: {action}, Error: {str(e)}")
            raise
    
    def upload_document(self, file_url: str, file_name: str, metadata: Optional[Dict] = None) -> str:
        """异步上传文档到AnalyticDB"""
        params = {
            'DBInstanceId': self.instance_id,
            'Collection': self.collection,
            'Namespace': self.namespace,
            'NamespacePassword': self.namespace_password,
            'FileName': file_name,
            'FileUrl': file_url,
            'ChunkSize': 500,  # 可以根据需要调整
            'ChunkOverlap': 50,
            'TextSplitterName': 'ChineseRecursiveTextSplitter',
            'DocumentLoaderName': 'UnstructuredFileLoader'
        }
        
        # 添加元数据
        if metadata:
            params['Metadata'] = json.dumps(metadata)
        
        try:
            result = self._make_request('UploadDocumentAsync', params)
            job_id = result.get('JobId')
            logger.info(f"文档上传任务已提交 - JobId: {job_id}, 文件: {file_name}")
            return job_id
            
        except Exception as e:
            logger.error(f"上传文档失败 - 文件: {file_name}, 错误: {str(e)}")
            raise
    
    def delete_document(self, file_name: str) -> bool:
        """删除AnalyticDB中的文档"""
        params = {
            'DBInstanceId': self.instance_id,
            'Collection': self.collection,
            'Namespace': self.namespace,
            'NamespacePassword': self.namespace_password,
            'FileName': file_name
        }
        
        try:
            result = self._make_request('DeleteDocument', params)
            status = result.get('Status')
            if status == 'success':
                logger.info(f"文档删除成功 - 文件: {file_name}")
                return True
            else:
                logger.error(f"文档删除失败 - 文件: {file_name}, 状态: {status}")
                return False
                
        except Exception as e:
            logger.error(f"删除文档失败 - 文件: {file_name}, 错误: {str(e)}")
            return False

class OSSEventProcessor:
    """OSS事件处理器"""
    
    def __init__(self):
        self.adb_client = AnalyticDBClient()
        
    def _extract_file_info_from_event(self, event: Dict[str, Any]) -> Dict[str, str]:
        """从OSS事件中提取文件信息"""
        try:
            # OSS触发器事件结构
            records = event.get('events', [])
            if not records:
                raise ValueError("事件中没有找到records")
            
            record = records[0]  # 通常只有一个记录
            oss_info = record.get('oss', {})
            
            bucket_name = oss_info.get('bucket', {}).get('name', '')
            object_key = urllib.parse.unquote(oss_info.get('object', {}).get('key', ''))
            event_name = record.get('eventName', '')
            
            # 生成文件URL（假设bucket是公开访问的，实际可能需要生成签名URL）
            file_url = f"https://{bucket_name}.oss-{self.adb_client.region_id}.aliyuncs.com/{urllib.parse.quote(object_key)}"
            
            return {
                'bucket_name': bucket_name,
                'object_key': object_key,
                'file_name': object_key.split('/')[-1],  # 提取文件名
                'file_url': file_url,
                'event_name': event_name
            }
            
        except Exception as e:
            logger.error(f"解析OSS事件失败: {str(e)}")
            raise
    
    def _should_process_file(self, object_key: str) -> bool:
        """判断是否应该处理该文件"""
        # 检查文件是否在指定前缀下
        if not object_key.startswith(self.adb_client.prefix_filter):
            logger.info(f"文件不在处理范围内: {object_key}")
            return False
        
        # 检查文件扩展名
        supported_extensions = ['.md', '.txt', '.pdf', '.docx', '.doc', '.html', '.json', '.csv']
        file_ext = os.path.splitext(object_key.lower())[1]
        
        if file_ext not in supported_extensions:
            logger.info(f"不支持的文件类型: {object_key}")
            return False
        
        return True
    
    def _extract_metadata_from_path(self, object_key: str) -> Dict[str, str]:
        """从文件路径中提取元数据"""
        # 假设路径格式为: wiki/space_name/document_name.md
        path_parts = object_key.split('/')
        
        metadata = {
            'source': 'feishu_wiki',
            'sync_timestamp': str(int(__import__('time').time()))
        }
        
        if len(path_parts) >= 2:
            metadata['space'] = path_parts[1]  # 知识空间名称
        
        if len(path_parts) >= 3:
            metadata['title'] = os.path.splitext(path_parts[-1])[0]  # 文件标题（不含扩展名）
        
        return metadata
    
    def process_create_event(self, file_info: Dict[str, str]) -> Dict[str, Any]:
        """处理文件创建事件"""
        logger.info(f"处理文件创建事件: {file_info['object_key']}")
        
        if not self._should_process_file(file_info['object_key']):
            return {'action': 'skip', 'reason': 'file not in scope'}
        
        try:
            # 提取元数据
            metadata = self._extract_metadata_from_path(file_info['object_key'])
            metadata['event_type'] = 'create'
            
            # 上传文档到AnalyticDB
            job_id = self.adb_client.upload_document(
                file_url=file_info['file_url'],
                file_name=file_info['file_name'],
                metadata=metadata
            )
            
            return {
                'action': 'upload',
                'status': 'success',
                'job_id': job_id,
                'file_name': file_info['file_name']
            }
            
        except Exception as e:
            logger.error(f"处理文件创建事件失败: {str(e)}")
            return {
                'action': 'upload',
                'status': 'failed',
                'error': str(e),
                'file_name': file_info['file_name']
            }
    
    def process_update_event(self, file_info: Dict[str, str]) -> Dict[str, Any]:
        """处理文件更新事件（先删除再创建）"""
        logger.info(f"处理文件更新事件: {file_info['object_key']}")
        
        if not self._should_process_file(file_info['object_key']):
            return {'action': 'skip', 'reason': 'file not in scope'}
        
        try:
            # 步骤1：删除旧文档
            delete_success = self.adb_client.delete_document(file_info['file_name'])
            
            # 步骤2：上传新文档
            metadata = self._extract_metadata_from_path(file_info['object_key'])
            metadata['event_type'] = 'update'
            
            job_id = self.adb_client.upload_document(
                file_url=file_info['file_url'],
                file_name=file_info['file_name'],
                metadata=metadata
            )
            
            return {
                'action': 'update',
                'status': 'success',
                'delete_success': delete_success,
                'upload_job_id': job_id,
                'file_name': file_info['file_name']
            }
            
        except Exception as e:
            logger.error(f"处理文件更新事件失败: {str(e)}")
            return {
                'action': 'update',
                'status': 'failed',
                'error': str(e),
                'file_name': file_info['file_name']
            }
    
    def process_delete_event(self, file_info: Dict[str, str]) -> Dict[str, Any]:
        """处理文件删除事件"""
        logger.info(f"处理文件删除事件: {file_info['object_key']}")
        
        if not self._should_process_file(file_info['object_key']):
            return {'action': 'skip', 'reason': 'file not in scope'}
        
        try:
            # 从AnalyticDB中删除文档
            success = self.adb_client.delete_document(file_info['file_name'])
            
            return {
                'action': 'delete',
                'status': 'success' if success else 'failed',
                'file_name': file_info['file_name']
            }
            
        except Exception as e:
            logger.error(f"处理文件删除事件失败: {str(e)}")
            return {
                'action': 'delete',
                'status': 'failed',
                'error': str(e),
                'file_name': file_info['file_name']
            }

def handler(event, context):
    """函数计算OSS触发器入口点"""
    logger.info("开始处理OSS触发事件")
    logger.info(f"请求ID: {context.request_id}")
    logger.info(f"事件内容: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        processor = OSSEventProcessor()
        
        # 解析文件信息
        file_info = processor._extract_file_info_from_event(event)
        event_name = file_info['event_name']
        
        logger.info(f"处理事件: {event_name}, 文件: {file_info['object_key']}")
        
        # 根据事件类型进行不同处理
        if 'ObjectCreated' in event_name:
            result = processor.process_create_event(file_info)
        elif 'ObjectModified' in event_name:
            result = processor.process_update_event(file_info)
        elif 'ObjectRemoved' in event_name:
            result = processor.process_delete_event(file_info)
        else:
            logger.warning(f"不支持的事件类型: {event_name}")
            result = {'action': 'unsupported', 'event_name': event_name}
        
        # 构建响应
        response = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'success': True,
                'message': 'OSS事件处理完成',
                'event_name': event_name,
                'file_info': file_info,
                'result': result
            }, ensure_ascii=False)
        }
        
        logger.info(f"事件处理完成: {json.dumps(result, ensure_ascii=False)}")
        return response
        
    except Exception as e:
        error_response = {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'success': False,
                'message': f'OSS事件处理失败: {str(e)}',
                'error': str(e)
            }, ensure_ascii=False)
        }
        
        logger.error(f"OSS事件处理失败: {str(e)}")
        return error_response
