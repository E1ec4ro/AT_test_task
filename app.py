import os
import uuid
import random
import logging
from flask import Flask, render_template, request, send_file, jsonify, session, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_mail import Mail, Message
from dotenv import load_dotenv
from ai_logic import AIProcessor
from excel_handler import ExcelHandler
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import func

load_dotenv()

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_UPLOADS = os.path.join(_BASE_DIR, 'uploads')

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")
app.config['UPLOAD_FOLDER'] = _DEFAULT_UPLOADS
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
_LOG_DIR = os.path.join(_BASE_DIR, 'logs')
os.makedirs(_LOG_DIR, exist_ok=True)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

API_KEY = os.getenv("AITUNNEL_API_KEY")
if not API_KEY:
    print("WARNING: AITUNNEL_API_KEY not found in .env")

processor = AIProcessor(api_key=API_KEY)

app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
app.config['MAIL_USE_SSL'] = os.getenv('MAIL_USE_SSL', 'False') == 'True'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME', 'electro_ded@inbox.ru')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
mail = Mail(app)
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL', 'electro_ded@inbox.ru').strip().lower()

logging.basicConfig(
    filename=os.path.join(_LOG_DIR, "app.log"),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    is_verified = db.Column(db.Boolean, default=False)
    auth_code = db.Column(db.String(6))
    code_expiry = db.Column(db.DateTime)
    last_login = db.Column(db.DateTime, default=datetime.now)
    last_use = db.Column(db.DateTime)
    is_banned = db.Column(db.Boolean, default=False)
    session_id = db.Column(db.String(100))

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class UserActivity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_email = db.Column(db.String(120), nullable=False)
    action = db.Column(db.String(64), nullable=False)
    details = db.Column(db.Text)
    ip_address = db.Column(db.String(64))
    user_agent = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.now)


def log_user_activity(user_email: str, action: str, details: str = None):
    activity = UserActivity(
        user_email=user_email or "anonymous",
        action=action,
        details=details,
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent', '')[:255]
    )
    db.session.add(activity)
    db.session.commit()
    logging.info("user=%s action=%s details=%s", user_email, action, details)


with app.app_context():
    db.create_all()


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email_or_login = request.form.get('email').strip().lower()
        password = request.form.get('password')
        action = request.form.get('action')

        if not email_or_login or not password:
            return render_template('login.html', error="Заполните все поля")

        user = User.query.filter_by(email=email_or_login).first()

        if action == 'register':
            if user:
                return render_template('login.html', error="Пользователь с такой почтой уже существует")

            user = User(email=email_or_login)
            user.set_password(password)
            code = str(random.randint(100000, 999999))
            user.auth_code = code
            user.code_expiry = datetime.now() + timedelta(minutes=10)
            db.session.add(user)
            db.session.commit()

            try:
                msg = Message("Код подтверждения - Интеллектуальный помощник",
                              sender=app.config['MAIL_USERNAME'],
                              recipients=[email_or_login])
                msg.body = f"Ваш код для подтверждения почты: {code}\nКод действует 10 минут."
                mail.send(msg)
                log_user_activity(email_or_login, 'register_code_sent')
                return redirect(url_for('verify', email=email_or_login))
            except Exception as e:
                logging.exception("Registration mail send failed for %s", email_or_login)
                return render_template('login.html', error=f"Ошибка отправки письма: {str(e)}")

        else:
            if not user or not user.check_password(password):
                return render_template('login.html', error="Неверная почта или пароль")

            if not user.is_verified:
                code = str(random.randint(100000, 999999))
                user.auth_code = code
                user.code_expiry = datetime.now() + timedelta(minutes=10)
                db.session.commit()

                msg = Message("Подтвердите почту - Интеллектуальный помощник",
                              sender=app.config['MAIL_USERNAME'],
                              recipients=[email_or_login])
                msg.body = f"Ваш код для подтверждения почты: {code}"
                mail.send(msg)
                log_user_activity(email_or_login, 'login_code_resent')
                return redirect(url_for('verify', email=email_or_login))

            session['user_email'] = email_or_login
            user.last_login = datetime.now()
            user.session_id = str(uuid.uuid4())
            session['session_id'] = user.session_id

            db.session.commit()
            log_user_activity(email_or_login, 'login_success')
            return redirect(url_for('index'))

    return render_template('login.html')


@app.route('/verify', methods=['GET', 'POST'])
def verify():
    email = request.args.get('email')
    if request.method == 'POST':
        email = request.form.get('email')
        code = request.form.get('code')

        user = User.query.filter_by(email=email, auth_code=code).first()
        if user and user.code_expiry > datetime.now():
            user.is_verified = True
            user.auth_code = None
            user.last_login = datetime.now()
            session['user_email'] = email
            db.session.commit()
            log_user_activity(email, 'verify_success')
            return redirect(url_for('index'))
        if email:
            log_user_activity(email, 'verify_failed')
        return render_template('verify.html', email=email, error="Неверный код или срок действия истек")

    return render_template('verify.html', email=email)


@app.route('/')
def index():
    user_email = session.get('user_email')
    if not user_email:
        return redirect(url_for('login'))

    user = User.query.filter_by(email=user_email).first()
    if not user or user.is_banned or user.session_id != session.get('session_id'):
        session.clear()
        return redirect(url_for('login', error="Сессия завершена или аккаунт заблокирован"))

    is_admin = user.email.lower() == ADMIN_EMAIL
    return render_template('index.html', is_admin=is_admin)


@app.route('/admin')
def admin_panel():
    user_email = session.get('user_email')
    if not user_email:
        return redirect(url_for('login'))
    if user_email.lower() != ADMIN_EMAIL:
        return redirect(url_for('index'))

    recent_logins = (
        User.query
        .filter(User.last_login.isnot(None))
        .order_by(User.last_login.desc())
        .limit(50)
        .all()
    )

    usage_stats = (
        db.session.query(
            UserActivity.user_email,
            func.count(UserActivity.id).label('usage_count'),
            func.max(UserActivity.created_at).label('last_usage')
        )
        .filter(UserActivity.action == 'ai_process_success')
        .group_by(UserActivity.user_email)
        .order_by(func.count(UserActivity.id).desc())
        .all()
    )

    recent_activities = (
        UserActivity.query
        .order_by(UserActivity.created_at.desc())
        .limit(100)
        .all()
    )

    return render_template(
        'admin.html',
        admin_email=user_email,
        recent_logins=recent_logins,
        usage_stats=usage_stats,
        recent_activities=recent_activities
    )


@app.route('/logout')
def logout():
    if session.get('user_email'):
        log_user_activity(session.get('user_email'), 'logout')
    session.pop('user_email', None)
    return redirect(url_for('login'))


@app.route('/process', methods=['POST'])
def process():
    user_email = session.get('user_email')
    if not user_email:
        return jsonify({'error': 'Неавторизован'}), 401

    user = User.query.filter_by(email=user_email).first()
    if not user or user.is_banned or user.session_id != session.get('session_id'):
        session.clear()
        return jsonify({'error': 'session_expired', 'redirect': url_for('login', error="Сессия завершена или аккаунт заблокирован")}), 401

    try:
        text_content = request.form.get('text', '')
        files = request.files.getlist('files')
        existing_excel = request.files.get('existing_excel')

        saved_paths = []
        for file in files:
            if file.filename:
                filename = secure_filename(f"{uuid.uuid4()}_{file.filename}")
                path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(path)
                saved_paths.append(path)

        existing_excel_path = None
        if existing_excel and existing_excel.filename:
            ex_filename = secure_filename(f"base_{uuid.uuid4().hex[:8]}_{existing_excel.filename}")
            existing_excel_path = os.path.join(app.config['UPLOAD_FOLDER'], ex_filename)
            existing_excel.save(existing_excel_path)

        results = processor.process_content(text=text_content, file_paths=saved_paths)

        user = User.query.filter_by(email=user_email).first()
        if user:
            user.last_use = datetime.now()
            db.session.commit()

        if not results:
            log_user_activity(user_email, 'ai_process_no_results')
            return jsonify({'error': 'Компании не найдены в предоставленных данных.'}), 404

        output_filename = f"result_{uuid.uuid4().hex[:8]}.xlsx"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)

        if existing_excel_path:
            import shutil
            shutil.copy(existing_excel_path, output_path)
            ExcelHandler.append_to_existing(output_path, results)
        else:
            ExcelHandler.create_new(output_path, results)

        for p in saved_paths:
            if os.path.exists(p):
                os.remove(p)
        if existing_excel_path and os.path.exists(existing_excel_path):
            os.remove(existing_excel_path)

        log_user_activity(user_email, 'ai_process_success', details=f"records={len(results)}")
        return jsonify({'download_url': f'/download/{output_filename}'})

    except Exception as e:
        logging.exception("Processing failed for user=%s", user_email)
        if user_email:
            log_user_activity(user_email, 'ai_process_error', details=str(e)[:500])
        return jsonify({'error': str(e)}), 500


@app.route('/download/<filename>')
def download(filename):
    if not session.get('user_email'):
        return "Неавторизован", 401
    safe = os.path.basename(filename)
    if not safe or safe != filename:
        return "Некорректное имя файла", 400
    path = os.path.join(app.config['UPLOAD_FOLDER'], safe)
    if not os.path.isfile(path):
        return "Файл не найден или срок хранения истёк", 404
    try:
        return send_file(path, as_attachment=True)
    except FileNotFoundError:
        return "Файл не найден", 404


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
