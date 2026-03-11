"""
認証・セッション管理・セキュリティモジュール
- bcryptパスワード認証
- セッションタイムアウト
- ログイン試行レート制限
- アクセスログ記録
- URL・入力値サニタイズ
"""

import hashlib
import hmac
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import streamlit as st

# bcryptが利用可能ならbcrypt、なければhmac-sha256フォールバック
try:
    import bcrypt
    _HAS_BCRYPT = True
except ImportError:
    _HAS_BCRYPT = False


# ============================================================
# 定数
# ============================================================
_MAX_LOGIN_ATTEMPTS = 5
_LOCKOUT_MINUTES = 15
_SESSION_TIMEOUT_MINUTES = 60
_DEFAULT_PASSWORD = "match2024"  # 初回セットアップ用（secrets.toml未設定時のフォールバック）


# ============================================================
# パスワードハッシュ生成ユーティリティ
# ============================================================
def generate_password_hash(password: str) -> str:
    """パスワードのハッシュを生成（セットアップ用）"""
    if _HAS_BCRYPT:
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    else:
        salt = os.urandom(16).hex()
        h = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
        return f"sha256:{salt}:{h}"


def _verify_password(password: str, password_hash: str) -> bool:
    """パスワードをハッシュと照合"""
    if _HAS_BCRYPT and password_hash.startswith("$2"):
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    elif password_hash.startswith("sha256:"):
        parts = password_hash.split(":", 2)
        if len(parts) != 3:
            return False
        salt, stored_hash = parts[1], parts[2]
        computed = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
        return hmac.compare_digest(computed, stored_hash)
    else:
        # プレーンテキスト比較（非推奨、secrets.toml移行前のフォールバック）
        return hmac.compare_digest(password, password_hash)


def _get_password_hash() -> str:
    """secrets.toml または環境変数からパスワードハッシュを取得"""
    # 1. secrets.toml から取得
    try:
        return st.secrets["auth"]["password_hash"]
    except (KeyError, FileNotFoundError, AttributeError):
        pass

    # 2. 環境変数から取得
    env_hash = os.environ.get("MATCH_PASSWORD_HASH")
    if env_hash:
        return env_hash

    # 3. フォールバック: デフォルトパスワードのハッシュ生成
    if _HAS_BCRYPT:
        return bcrypt.hashpw(_DEFAULT_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    else:
        salt = os.urandom(16).hex()
        h = hashlib.sha256((salt + _DEFAULT_PASSWORD).encode("utf-8")).hexdigest()
        return f"sha256:{salt}:{h}"


# ============================================================
# ログイン試行レート制限
# ============================================================
def _check_rate_limit() -> tuple[bool, int]:
    """レート制限チェック。(許可, 残り秒数) を返す。"""
    if "lockout_until" in st.session_state:
        lockout = st.session_state["lockout_until"]
        if datetime.now() < lockout:
            remaining = int((lockout - datetime.now()).total_seconds())
            return False, remaining
        else:
            # ロックアウト期限切れ → リセット
            del st.session_state["lockout_until"]
            st.session_state["login_attempts"] = 0

    return True, 0


def _record_failed_attempt():
    """失敗試行を記録"""
    attempts = st.session_state.get("login_attempts", 0) + 1
    st.session_state["login_attempts"] = attempts

    if attempts >= _MAX_LOGIN_ATTEMPTS:
        st.session_state["lockout_until"] = datetime.now() + timedelta(minutes=_LOCKOUT_MINUTES)
        # アクセスログ
        _log_access("login_lockout", f"{attempts}回の失敗によりロックアウト")


def _log_access(event_type: str, detail: str = ""):
    """アクセスログをDBに記録"""
    try:
        from cache_manager import add_access_log
        add_access_log(event_type, detail)
    except (ImportError, Exception):
        pass


# ============================================================
# メイン認証関数
# ============================================================
def _get_correct_password() -> str:
    """secrets.toml / 環境変数 / デフォルトからパスワードを取得"""
    # 1. secrets.toml の password キー（プレーン）
    try:
        return st.secrets["auth"]["password"]
    except (KeyError, FileNotFoundError, AttributeError):
        pass
    # 2. 環境変数
    env_pw = os.environ.get("MATCH_PASSWORD")
    if env_pw:
        return env_pw
    # 3. デフォルト
    return _DEFAULT_PASSWORD


def check_password() -> bool:
    """パスワード認証画面を表示。認証済みならTrueを返す。"""

    # 既に認証済みか確認
    if st.session_state.get("authenticated") is True:
        return True

    # ロゴ・タイトル
    st.markdown("""
    <div style="text-align:center; padding:3rem 0 1rem;">
        <div style="font-size:3rem;">🎯</div>
        <h1 style="color:#1F4E79; margin:0.5rem 0;">Match</h1>
        <p style="color:#718096;">人材マッチングシステム</p>
    </div>
    """, unsafe_allow_html=True)

    # レート制限チェック
    allowed, remaining = _check_rate_limit()
    if not allowed:
        st.error(f"🔒 ログイン試行回数の上限に達しました。{remaining}秒後に再試行してください。")
        return False

    # ログインフォーム
    with st.form("login_form"):
        st.markdown("### 🔐 ログイン")
        password = st.text_input("パスワード", type="password", placeholder="パスワードを入力してください")
        submitted = st.form_submit_button("ログイン", type="primary", use_container_width=True)

    if submitted:
        if not password:
            st.warning("パスワードを入力してください。")
            return False

        correct_password = _get_correct_password()
        is_valid = hmac.compare_digest(password, correct_password)

        if is_valid:
            st.session_state["authenticated"] = True
            st.session_state["auth_time"] = datetime.now().isoformat()
            st.session_state["last_activity"] = datetime.now().isoformat()
            st.session_state["login_attempts"] = 0
            _log_access("login_success", "ログイン成功")
            st.rerun()
        else:
            _record_failed_attempt()
            attempts = st.session_state.get("login_attempts", 0)
            remaining_attempts = _MAX_LOGIN_ATTEMPTS - attempts
            if remaining_attempts > 0:
                st.error(f"❌ パスワードが正しくありません。残り{remaining_attempts}回")
            else:
                st.error("🔒 ログイン試行回数の上限に達しました。しばらくお待ちください。")
            _log_access("login_failed", f"失敗 (試行{attempts}回目)")
            return False

    # フッター
    st.markdown("""
    <div style="text-align:center; padding:2rem 0; color:#a0aec0; font-size:0.8rem;">
        <p>🔒 このシステムは認証が必要です</p>
        <p>アクセス権がない場合は管理者にお問い合わせください</p>
    </div>
    """, unsafe_allow_html=True)

    return False


# ============================================================
# セッション管理
# ============================================================
def check_session_timeout():
    """セッションタイムアウトチェック。タイムアウト時はセッションをクリア。"""
    if not st.session_state.get("authenticated"):
        return

    last_activity = st.session_state.get("last_activity")
    if not last_activity:
        return

    try:
        last_dt = datetime.fromisoformat(last_activity)
        timeout = timedelta(minutes=_SESSION_TIMEOUT_MINUTES)
        if datetime.now() - last_dt > timeout:
            _log_access("session_timeout", f"最終操作から{_SESSION_TIMEOUT_MINUTES}分経過")
            logout()
            st.warning("⏰ セッションがタイムアウトしました。再度ログインしてください。")
            st.stop()
    except (ValueError, TypeError):
        pass

    # アクティビティ更新
    st.session_state["last_activity"] = datetime.now().isoformat()


def logout():
    """ログアウト処理"""
    _log_access("logout", "ログアウト")
    for key in list(st.session_state.keys()):
        del st.session_state[key]


def render_logout_button():
    """サイドバーにログアウトボタンを表示"""
    if st.session_state.get("authenticated"):
        auth_time = st.session_state.get("auth_time", "")[:16].replace("T", " ")
        st.sidebar.caption(f"🔐 ログイン: {auth_time}")
        if st.sidebar.button("🚪 ログアウト", key="sidebar_logout"):
            logout()
            st.rerun()


# ============================================================
# 入力サニタイズ
# ============================================================
def safe_url(url: str) -> Optional[str]:
    """URLの安全性を検証。安全ならURLを返し、危険ならNone。"""
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    # http/https のみ許可
    if not url.startswith(("http://", "https://")):
        return None
    # javascript: / data: / vbscript: スキームをブロック
    lower = url.lower()
    dangerous = ["javascript:", "data:", "vbscript:", "file:"]
    if any(lower.startswith(d) for d in dangerous):
        return None
    return url


def sanitize_input(text: str, max_length: int = 10000) -> str:
    """テキスト入力をサニタイズ"""
    if not text or not isinstance(text, str):
        return ""
    # 長さ制限
    text = text[:max_length]
    # NULL バイト除去
    text = text.replace("\x00", "")
    return text.strip()
