"""
高硅铁尾矿复合胶凝材料数据采集系统
版本：2.0
新增功能：
1. 交互式菜单系统
2. 支持批量处理多种文件格式（PDF/DOCX/TXT）
3. 网络爬虫模块
4. 异常处理增强
"""

# ----------------------
# 第一部分：基础配置区
# ----------------------
DATABASE_NAME = "material_data.db"
SUPPORTED_EXT = ['.pdf', '.docx', '.txt']  # 支持文件夹批量处理的文件类型
CRAWL_SEED_URL = "http://example.com/materials"  # 爬虫种子URL
#爬虫种子URL为示例，需根据实际网站修改，种子设定好后，可在crawl方法中修改解析规则
#爬虫的爬取逻辑是从当前种子地址开始，爬取页面所包含的所有连接，然后逐一访问这些连接，提取数据
#提取数据的逻辑在_parse_page方法中，需要根据实际网站结构修改
#删除mytest1.py文件中提取的数据必须包含数据库中所有字段的限制
# ----------------------

# ----------------------
# 模块一：文件处理器
# ----------------------
import os
import docx
import fitz  # PyMuPDF

class FileProcessor:
    @staticmethod
    def read_pdf(filepath: str) -> str:
        """读取PDF文件（包含表格解析）"""
        text = ""
        try:
            with fitz.open(filepath) as doc:
                for page in doc:
                    text += page.get_text()
                    # 提取表格（示例）
                    for table in page.find_tables():
                        text += "\n表格数据：" + str(table.extract())
        except Exception as e:
            print(f"PDF读取错误：{str(e)}")
        return text

    @staticmethod
    def read_docx(filepath: str) -> str:
        """读取DOCX文件"""
        try:
            doc = docx.Document(filepath)
            return "\n".join([para.text for para in doc.paragraphs])
        except Exception as e:
            print(f"DOCX读取错误：{str(e)}")
            return ""

    @staticmethod
    def read_txt(filepath: str) -> str:
        """读取TXT文件"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"TXT读取错误：{str(e)}")
            return ""

# ----------------------
# 模块二：网络爬虫
# ----------------------
import requests
from bs4 import BeautifulSoup

class MaterialCrawler:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def _parse_page(self, html: str) -> list:
        """解析网页内容（需根据实际网站结构修改）"""
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        for item in soup.select('.material-item'):
            data = {
                'title': item.find('h3').text.strip(),
                'content': item.find('div', class_='content').text.strip()
            }
            results.append(data)
        return results

    def crawl(self, max_pages=3):
        """执行爬虫任务"""
        for page in range(1, max_pages+1):
            url = f"{self.base_url}?page={page}"
            try:
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    yield from self._parse_page(response.text)
                else:
                    print(f"请求失败：{url}")
            except Exception as e:
                print(f"爬虫错误：{str(e)}")

# ----------------------
# 模块三：数据库
# ----------------------
import sqlite3
import re
import spacy
from typing import Dict, Optional
from tqdm import tqdm
import matplotlib.pyplot as plt
from reportlab.pdfgen import canvas
import smtplib
import os.path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from email.mime.text import MIMEText


def show_strength_analysis():
    # 从数据库读取强度数据并绘制图表
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT compressive_strength_28d FROM materials")
    data = cursor.fetchall()
    conn.close()
    
    strengths = [row[0] for row in data if row[0] is not None]
    plt.hist(strengths, bins=20)
    plt.title("28天抗压强度分布")
    plt.xlabel("抗压强度（MPa）")
    plt.ylabel("频数")
    plt.show()

def generate_report():
    # 生成PDF格式的实验报告
    c = canvas.Canvas("report.pdf")
    c.drawString(100, 750, "实验报告")
    c.save()

def send_alert(email):
    # 发送处理完成通知
    server = smtplib.SMTP('smtp.example.com', 587)
    server.starttls()
    server.login("your_email@example.com", "your_password")
    message = "Subject: 数据处理完成\n\n数据处理已完成。"
    server.sendmail("your_email@example.com", email, message)
    server.quit()

class MaterialDataProcessor:
    def __init__(self):
        int_1 = False  #当发生错误1时,改为Ture;当错误1解决后,改回False
        
        # 初始化数据库连接
        self.conn = sqlite3.connect(DATABASE_NAME)
        
        # 检查是否需要更新数据库表结构
        if int_1:
            self._update_table()         
        
        self._create_table()
        
        # 加载NLP模型（首次使用需先运行：python -m spacy download en_core_web_sm）
        #已运行过，不需要再次运行
        self.nlp = spacy.load("en_core_web_sm")

    # ----------------------
    # 第二部分：网络爬虫
    # ----------------------
    def _create_table(self):
        """创建数据库表结构（字段需要时扩展）"""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,      -- 主键ID，自动递增
                author TEXT,                               -- 作者
                title TEXT,                                -- 标题
                year INTEGER,                              -- 发表年份 
                -- 高硅铁尾矿原料特性（可添加更多字段）
                SiO2_content REAL,        -- SiO2含量（%）
                Al2O3_content REAL,       -- Al2O3含量（%）
                CaO_content REAL,         -- CaO含量（%）
                Fe2O3_content REAL,       -- Fe2O3含量（%）
                MgO_content REAL,         -- MgO含量（%）
                MnO_content REAL,         -- MnO含量（%）
                TiO2_content REAL,        -- TiO2含量（%）
                -- 高硅铁尾矿物化性质（可添加更多字段）
                slurry_concentration REAL,  -- 泥浆浓度（%）
                moisture_content REAL,      -- 水分含量（%）
                psd_0_50 REAL,              -- 0-50μm颗粒分布（%）
                psd_50_100 REAL,            -- 50-100μm颗粒分布（%）
                psd_100_200 REAL,           -- 100-200μm颗粒分布（%）
                psd_200_plus REAL,          -- >200μm颗粒分布（%）
                -- 高硅铁尾矿矿物组成（可添加更多字段）
                mineral_1 TEXT,             -- 矿物1
                -- 高硅铁尾矿碱活化剂种类（可添加更多字段）
                alkali_activator TEXT,      -- 碱活化剂
                -- 高硅铁尾矿胶凝材料配比（可添加更多字段）
                cement_content REAL,        -- 水泥掺量（%）
                fly_ash_content REAL,       -- 粉煤灰掺量（%）
                water_binder_ratio REAL,    -- 水灰比
                superplasticizer_content REAL,  -- 减水剂掺量（%）
                alkali_activator_content REAL,  -- 碱活化剂掺量（%）
                -- 高硅铁尾矿胶凝材料制备工艺参数
                curing_temp INTEGER,        -- 养护温度（℃）
                curing_time REAL,           -- 养护时间（h）
                curing_humidity REAL,       -- 养护湿度（%）
                curing_method TEXT,         -- 养护方法
                curing_pressure REAL,       -- 养护压力（MPa）
                calcination_temp INTEGER,   -- 煅烧温度（℃）
                mixing_time INTEGER,        -- 混合时间（分钟）
                -- 高硅铁尾矿胶凝材料性能
                compressive_strength_28d REAL,  -- 28天抗压强度（MPa）
                flexural_strength_28d REAL,     -- 28天抗折强度（MPa）
                modulus_of_elaontent_28d REAL,  -- 28天氯离子含量（%）
                alkali_content_28d REAL,        -- 28天碱含量（%）
                -- 高硅铁尾矿胶凝材料耐久性能
                carbonation_depth REAL,         -- 碳化深度（mm）
                chloride_ion_content_depth REAL,  -- 氯离子渗透深度（mm）
                water_absorption_28d REAL,      -- 吸水率（%）
                sticity_28d REAL,               -- 28天弹性模量（GPa）
                drying_shrinkage_28d REAL,      -- 28天干缩率（%）
                chloride_ion_c REAL             -- 氯离子渗透系数（mm/s）
            )
        ''')
        self.conn.commit()

    def _update_table(self):
        """更新数据库表结构，扩充字段"""
        cursor = self.conn.cursor()
        # 若需要新增字段，可在new_columns中添加新字段的SQL语句
        # new_columns = ["字段名 数据类型", ...]
        # 若字段已存在，会抛出 OperationalError 异常，可忽略
        # 完成数据库表结构更新后，可随时删除new_columns中的字段
        new_columns = [
            "author TEXT",
            "title TEXT",
            "year INTEGER"  # 以上为示例，使用后请删除
        ]
        for column in new_columns:
            try:
                cursor.execute(f"ALTER TABLE materials ADD COLUMN {column}")
            except sqlite3.OperationalError as e:
                print(f"跳过已存在的列：{column}")
        print("数据库表结构已更新,将int_1设置为False!")
        self.conn.commit()

 # ----------------------
    # 第三部分：数据提取模块（需要根据实际数据格式修改）
    # ----------------------  
    def extract_from_text(self, text: str) -> Dict[str, float]:
        """从给定的文本中提取关键参数。"""
        # 使用Spacy进行文本处理（分句、分词等）
        doc = self.nlp(text)
        
        # 示例提取规则（需要根据实际文献格式修改正则表达式）
        return {
            "SiO2_content": self._find_float(text, r"SiO2[:：]\s*([\d.]+)%"),
            "Al2O3_content": self._find_float(text, r"Al2O3[:：]\s*([\d.]+)%"),
            "CaO_content": self._find_float(text, r"CaO[:：]\s*([\d.]+)%"),
            "Fe2O3_content": self._find_float(text, r"Fe2O3[:：]\s*([\d.]+)%"),
            "MgO_content": self._find_float(text, r"MgO[:：]\s*([\d.]+)%"),
            "MnO_content": self._find_float(text, r"MnO[:：]\s*([\d.]+)%"),
            "TiO2_content": self._find_float(text, r"TiO2[:：]\s*([\d.]+)%"),
            "slurry_concentration": self._find_float(text, r"泥浆浓度[:：]\s*([\d.]+)%"),
            "moisture_content": self._find_float(text, r"水分含量[:：]\s*([\d.]+)%"),
            "psd_0_50": self._find_float(text, r"0-50μm颗粒分布[:：]\s*([\d.]+)%"),
            "psd_50_100": self._find_float(text, r"50-100μm颗粒分布[:：]\s*([\d.]+)%"),
            "psd_100_200": self._find_float(text, r"100-200μm颗粒分布[:：]\s*([\d.]+)%"),
            "psd_200_plus": self._find_float(text, r">200μm颗粒分布[:：]\s*([\d.]+)%"),
            "mineral_1": self._find_text(text, r"矿物1[:：]\s*(\w+)"),
            "alkali_activator": self._find_text(text, r"碱活化剂[:：]\s*(\w+)"),
            "cement_content": self._find_float(text, r"水泥掺量[:：]\s*([\d.]+)%"),
            "fly_ash_content": self._find_float(text, r"粉煤灰掺量[:：]\s*([\d.]+)%"),
            "water_binder_ratio": self._find_float(text, r"水灰比[:：]\s*([\d.]+)"),
            "superplasticizer_content": self._find_float(text, r"减水剂掺量[:：]\s*([\d.]+)%"),
            "alkali_activator_content": self._find_float(text, r"碱活化剂掺量[:：]\s*([\d.]+)%"),
            "curing_temp": self._find_int(text, r"养护温度[:：]\s*(\d+)℃"),
            "curing_time": self._find_float(text, r"养护时间[:：]\s*([\d.]+)h"),
            "curing_humidity": self._find_float(text, r"养护湿度[:：]\s*([\d.]+)%"),
            "curing_method": self._find_text(text, r"养护方法[:：]\s*(\w+)"),
            "curing_pressure": self._find_float(text, r"养护压力[:：]\s*([\d.]+)MPa"),
            "calcination_temp": self._find_int(text, r"煅烧温度[:：]\s*(\d+)℃"),
            "mixing_time": self._find_int(text, r"混合时间[:：]\s*(\d+)分钟"),
            "compressive_strength_28d": self._find_float(text, r"28天抗压强度[:：]\s*([\d.]+)MPa"),
            "flexural_strength_28d": self._find_float(text, r"28天抗折强度[:：]\s*([\d.]+)MPa"),
            "modulus_of_elaontent_28d": self._find_float(text, r"28天氯离子含量[:：]\s*([\d.]+)%"),
            "alkali_content_28d": self._find_float(text, r"28天碱含量[:：]\s*([\d.]+)%"),
            "carbonation_depth": self._find_float(text, r"碳化深度[:：]\s*([\d.]+)mm"),
            "chloride_ion_content_depth": self._find_float(text, r"氯离子渗透深度[:：]\s*([\d.]+)mm"),
            "water_absorption_28d": self._find_float(text, r"吸水率[:：]\s*([\d.]+)%"),
            "sticity_28d": self._find_float(text, r"28天弹性模量[:：]\s*([\d.]+)GPa"),
            "drying_shrinkage_28d": self._find_float(text, r"28天干缩率[:：]\s*([\d.]+)%"),
            "chloride_ion_c": self._find_float(text, r"氯离子渗透系数[:：]\s*([\d.]+)mm/s")
        }

    # ----------------------
    # 正则表达式提取函数
    # ----------------------
    def _find_float(self, text: str, pattern: str) -> Optional[float]:
        """使用正则表达式查找浮点数"""
        match = re.search(pattern, text)
        return float(match.group(1)) if match else None

    def _find_int(self, text: str, pattern: str) -> Optional[int]:
        """使用正则表达式查找整数"""
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None

    def _find_text(self, text: str, pattern: str) -> Optional[str]:
        """使用正则表达式查找文本"""
        match = re.search(pattern, text)
        return match.group(1) if match else None

    # ----------------------
    # 第四部分：数据检查模块（需要根据业务规则修改）
    # ----------------------
    def validate_data(self, data: Dict) -> bool:
        """验证数据有效性（添加你的验证规则）"""
        # 相比mytest1.py，删除了检查必填字段
        
        # 数值范围检查（示例）
        if data.get("SiO2_content") and data["SiO2_content"] <= 64:
            print("错误：不属于高硅型铁尾矿")
            return False
            
        return True

    # ----------------------
    # 第五部分：数据存储模块（通常无需修改）
    # ----------------------
    def save_to_db(self, data: Dict):
        """将数据存入数据库，当数据库表结构拓展时，更新下面的SQL语句"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO materials (
                    SiO2_content,
                    Al2O3_content,
                    CaO_content,
                    Fe2O3_content,
                    MgO_content,
                    MnO_content,
                    TiO2_content,
                    slurry_concentration,
                    moisture_content,
                    psd_0_50,
                    psd_50_100,
                    psd_100_200,
                    psd_200_plus,
                    mineral_1,
                    alkali_activator,
                    cement_content,
                    fly_ash_content,
                    water_binder_ratio,
                    superplasticizer_content,
                    alkali_activator_content,
                    curing_temp,
                    curing_time,
                    curing_humidity,
                    curing_method,
                    curing_pressure,
                    calcination_temp,
                    mixing_time,
                    compressive_strength_28d,
                    flexural_strength_28d,
                    modulus_of_elaontent_28d,
                    alkali_content_28d,
                    carbonation_depth,
                    chloride_ion_content_depth,
                    water_absorption_28d,
                    sticity_28d,
                    drying_shrinkage_28d,
                    chloride_ion_c
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get("SiO2_content"),
                data.get("Al2O3_content"),
                data.get("CaO_content"),
                data.get("Fe2O3_content"),
                data.get("MgO_content"),
                data.get("MnO_content"),
                data.get("TiO2_content"),
                data.get("slurry_concentration"),
                data.get("moisture_content"),
                data.get("psd_0_50"),
                data.get("psd_50_100"),
                data.get("psd_100_200"),
                data.get("psd_200_plus"),
                data.get("mineral_1"),
                data.get("alkali_activator"),
                data.get("cement_content"),
                data.get("fly_ash_content"),
                data.get("water_binder_ratio"),
                data.get("superplasticizer_content"),
                data.get("alkali_activator_content"),
                data.get("curing_temp"),
                data.get("curing_time"),
                data.get("curing_humidity"),
                data.get("curing_method"),
                data.get("curing_pressure"),
                data.get("calcination_temp"),
                data.get("mixing_time"),
                data.get("compressive_strength_28d"),
                data.get("flexural_strength_28d"),
                data.get("modulus_of_elaontent_28d"),
                data.get("alkali_content_28d"),
                data.get("carbonation_depth"),
                data.get("chloride_ion_content_depth"),
                data.get("water_absorption_28d"),
                data.get("sticity_28d"),
                data.get("drying_shrinkage_28d"),
                data.get("chloride_ion_c")
            ))
            self.conn.commit()
            print("成功存入1条数据")
        except sqlite3.OperationalError as e:
            if "no column named" in str(e):
                print("错误1:数据库表结构未更新!")
                print("方法1:将int_1设置为True,同时将新增字段名填入new_columns,并运行程序!")
                print("方法2:手动删除数据库文件material_data.db,并运行程序!")
            else:
                print(f"数据库错误：{str(e)}")

    def process_folder(self, folder_path: str):
        """批量处理文件夹"""
        if not os.path.exists(folder_path):
            print("文件夹路径不存在！")
            return

        files = [f for f in os.listdir(folder_path) 
                if os.path.splitext(f)[1].lower() in SUPPORTED_EXT]
        
        with tqdm(total=len(files), desc="批量处理") as pbar:
            for filename in files:
                filepath = os.path.join(folder_path, filename)
                ext = os.path.splitext(filename)[1].lower()
                
                # 选择读取方式
                if ext == '.pdf':
                    text = FileProcessor.read_pdf(filepath)
                elif ext == '.docx':
                    text = FileProcessor.read_docx(filepath)
                else:
                    text = FileProcessor.read_txt(filepath)
                
                if text:
                    self._process_text(text)
                pbar.update(1)

    def _process_text(self, text: str):
        """统一处理文本内容"""
        extracted_data = self.extract_from_text(text)
        if self.validate_data(extracted_data):
            self.save_to_db(extracted_data)

    def run_crawler(self):
        """启动爬虫任务"""
        crawler = MaterialCrawler(CRAWL_SEED_URL)
        for data in crawler.crawl():
            # 将爬取内容转为标准格式
            processed = self._adapt_crawled_data(data)
            self._process_text(processed)

    def _adapt_crawled_data(self, data: dict) -> str:
        """将爬取数据转换为标准文本格式（需根据实际结构修改）"""
        return f"""
        标题：{data.get('title', '')}
        内容：{data.get('content', '')}
        """
    def get_credentials():
        SCOPES = ['https://www.googleapis.com/auth/gmail.send']
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        return creds

    def send_alert(email):
        creds = get_credentials()
        
        # 创建MIME多部分消息
        msg = MIMEText("数据处理已完成。")
        msg['Subject'] = '数据处理完成'
        msg['From'] = 'tsukusei51@gmail.com'
        msg['To'] = email
        
        try:
            # 使用OAuth 2.0获取授权
            service = smtplib.SMTP('smtp.gmail.com', 587)
            service.starttls()
            service.login('tsukusei51@gmail.com', creds.token)
            service.send_message(msg)
            print("邮件发送成功")
        except Exception as e:
            print(f"邮件发送失败: {e}")
        finally:
            service.quit()

# ----------------------
# 交互菜单系统
# ----------------------
def show_menu():
    print("\n" + "="*40)
    print("高硅铁尾矿数据采集系统")
    print("1. 批量处理文件夹")
    print("2. 启动网络爬虫")
    print("3. 退出系统")
    return input("请选择操作（1-3）：")
    
if __name__ == "__main__":
    processor = MaterialDataProcessor()
    
    while True:
        choice = show_menu()
        
        if choice == '1':
            folder = input("请输入文件夹路径：")
            processor.process_folder(folder)
        elif choice == '2':
            print("开始爬取数据...")
            processor.run_crawler()
        elif choice == '3':
            print("感谢使用，再见！")
            break
        else:
            print("无效输入，请重新选择！")
    
    processor.conn.close()