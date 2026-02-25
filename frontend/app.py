import os
import json
import logging
from collections import deque

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Dashboard")

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092"
).split(",")
BUFFER_SIZE = 1000


# Конфиг страницы 

st.set_page_config(
    page_title="Amazon Beauty Reviews Sentiment Dashboard",
    page_icon="💄",
    layout="wide",
)

st.title("Amazon Beauty Reviews Sentiment Dashboard")
st.caption("DistilBERT · Apache Kafka · 2 brokers · live stream")


# Kafka consumers с кэшированием, чтобы не пересоздавать при каждом рендере и не терять позицию в топике

@st.cache_resource
def get_ml_consumer():
    return KafkaConsumer(
        "ml-results",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="dashboard-ml-group",
        auto_offset_reset="earliest",
        consumer_timeout_ms=800,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

@st.cache_resource
def get_vis_consumer():
    return KafkaConsumer(
        "visualization-data",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="dashboard-vis-group",
        auto_offset_reset="earliest",
        consumer_timeout_ms=800,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

# Буферы в session_state для хранения считанных сообщений и отображения их в разных блоках дашборда

if "ml_buf" not in st.session_state:
    st.session_state.ml_buf = deque(maxlen=BUFFER_SIZE)
if "vis_buf" not in st.session_state:
    st.session_state.vis_buf = deque(maxlen=BUFFER_SIZE)

# Функция для вычитывания новых сообщений из Kafka — вызывается при загрузке страницы и при нажатии кнопки "Обновить"
def poll():
    if st.session_state.get("polling", False):
        return
    st.session_state["polling"] = True
    try:
        ml_consumer  = get_ml_consumer()
        vis_consumer = get_vis_consumer()

        # poll() вместо итерации — потокобезопасно
        ml_records  = ml_consumer.poll(timeout_ms=800, max_records=200)
        vis_records = vis_consumer.poll(timeout_ms=800, max_records=200)

        for tp, messages in ml_records.items():
            for msg in messages:
                st.session_state.ml_buf.append(msg.value)

        for tp, messages in vis_records.items():
            for msg in messages:
                st.session_state.vis_buf.append(msg.value)
    finally:
        st.session_state["polling"] = False


# Кнопка обновления

col_btn, col_info = st.columns([1, 5])
with col_btn:
    if st.button("Обновить"):
        poll()
with col_info:
    st.caption("Данные обновляются при нажатии кнопки")

poll()   # автоматически при каждом рендере

ml_data  = list(st.session_state.ml_buf)
vis_data = list(st.session_state.vis_buf)


# Блок 1: Общая статистика по предсказаниям и алертам

st.markdown("---")
st.subheader("Общая статистика")

if ml_data:
    df_ml = pd.DataFrame(ml_data)

    if "total_processed" in df_ml.columns:
        total = int(df_ml["total_processed"].max())
    else:
        total = len(df_ml)
    pos_pct  = (df_ml["predicted_label"] == "positive").mean() * 100
    neg_pct  = (df_ml["predicted_label"] == "negative").mean() * 100
    if "total_alerts" in df_ml.columns:
        alerts = int(df_ml["total_alerts"].max())
    else:
        alerts = int(df_ml["is_alert"].sum())
    avg_conf = df_ml["confidence"].mean()

    # Online accuracy — только для сообщений, у которых есть true_sentiment (в реальной системе это могут быть, например, отзывы с ручной модерацией)
    if "true_sentiment" in df_ml.columns:
        acc = (df_ml["true_sentiment"] == df_ml["predicted_label"]).mean() * 100
    else:
        acc = None

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Обработано",       f"{total:,}")
    k2.metric("Позитивных",       f"{pos_pct:.1f}%")
    k3.metric("Негативных",       f"{neg_pct:.1f}%")
    k4.metric("Алертов",       f"{alerts:,}")
    k5.metric("Online accuracy",  f"{acc:.1f}%" if acc else "—")

else:
    st.info("Ожидаем данные из ml-results...")


# Блок 2: Последние 20 отзывов с предсказаниями и алертами

st.markdown("---")
st.subheader("Последние 20 отзывов")

if ml_data:
    cols_recent = [
        "timestamp",
        "asin",
        "title",
        "text",
        "predicted_label",
        "confidence",
        "aspect_tags",
        "producer",
    ]

    recent_df = (
        df_ml
        .tail(20)  # реально последние 20 сообщений из потока
        .loc[:, [c for c in cols_recent if c in df_ml.columns]]
        .reset_index(drop=True)
    )

    if not recent_df.empty:
        for col in cols_recent:
            if col not in recent_df.columns:
                recent_df[col] = ""

        # Преобразуем список аспектов в строку для табличного отображения
        if "aspect_tags" in recent_df.columns:
            recent_df["aspect_tags"] = recent_df["aspect_tags"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else (x or "")
            )

        if "title" in recent_df.columns:
            recent_df["title"] = recent_df["title"].fillna("").str.slice(0, 80)
        if "text" in recent_df.columns:
            recent_df["text"] = recent_df["text"].fillna("").str.slice(0, 140)

        st.dataframe(recent_df, use_container_width=True, height=500)
    else:
        st.info("Пока нет отзывов для отображения")
else:
    st.info("Ожидаем данные из ml-results...")


# Блок 3: Распределение предсказаний (пирог и бар)

st.markdown("---")
st.subheader("Распределение предсказаний")

if ml_data:
    col1, col2 = st.columns(2)

    # Pie: positive vs negative
    with col1:
        counts = df_ml["predicted_label"].value_counts().reset_index()
        counts.columns = ["sentiment", "count"]
        fig_pie = px.pie(
            counts,
            names="sentiment",
            values="count",
            color="sentiment",
            color_discrete_map={
                "positive": "#2e7d32",
                "negative": "#d32f2f",
            },
            title="Predicted Sentiment",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # Bar: распределение предсказанного сентимента по продьюсерам
    with col2:
        if "producer" in df_ml.columns:
            prod_counts = (
                df_ml
                .groupby(["producer", "predicted_label"])
                .size()
                .reset_index(name="count")
            )
            fig_bar = px.bar(
                prod_counts,
                x="producer",
                y="count",
                color="predicted_label",
                barmode="stack",
                title="Распределение предсказаний по продьюсерам",
                labels={"producer": "Producer", "count": "Count", "predicted_label": "Sentiment"},
                color_discrete_map={
                    "positive": "#2e7d32",
                    "negative": "#d32f2f",
                },
            )
            st.plotly_chart(fig_bar, use_container_width=True)


# Блок 4: Динамика среднего рейтинга и доли негативных отзывов в скользящем окне

st.markdown("---")
st.subheader("Rolling Window (последние 500 отзывов)")

if vis_data:
    df_vis = pd.DataFrame(vis_data)
    # Берем только последние 500 точек, чтобы графики были более «живыми» и не перегружали интерфейс
    df_vis = df_vis.tail(500).reset_index(drop=True)
    df_vis["x"] = range(len(df_vis))

    col3, col4 = st.columns(2)

    with col3:
        fig_avg = go.Figure()
        fig_avg.add_trace(go.Scatter(
            x=df_vis["x"],
            y=df_vis["avg_rating"],
            mode="lines",
            name="Avg Rating",
            line=dict(color="#1a3d8f", width=2),
            fill="tozeroy",
            fillcolor="rgba(26,61,143,0.1)",
        ))
        fig_avg.update_layout(
            title="Средний рейтинг (скользящее окно)",
            xaxis_title="Snapshot #",
            yaxis_title="Avg Rating",
            yaxis=dict(range=[1, 5]),
        )
        st.plotly_chart(fig_avg, use_container_width=True)

    with col4:
        fig_neg = go.Figure()
        fig_neg.add_trace(go.Scatter(
            x=df_vis["x"],
            y=df_vis["neg_share_pct"],
            mode="lines",
            name="Negative %",
            line=dict(color="#d32f2f", width=2),
            fill="tozeroy",
            fillcolor="rgba(211,47,47,0.15)",
        ))
        fig_neg.update_layout(
            title="Доля негативных отзывов % (скользящее окно)",
            xaxis_title="Snapshot #",
            yaxis_title="%",
            yaxis=dict(range=[0, 100]),
        )
        st.plotly_chart(fig_neg, use_container_width=True)

else:
    st.info("Ожидаем данные из visualization-data...")


# Блок 5: Гистограмма уверенности модели и алерт по порогу

st.markdown("---")
st.subheader("Уверенность модели")

if ml_data:
    # Берем последние 500 предсказаний для анализа распределения уверенности
    df_conf = df_ml.tail(500).reset_index(drop=True)

    fig_conf = px.histogram(
        df_conf,
        x="confidence",
        color="predicted_label",
        nbins=40,
        barmode="overlay",
        opacity=0.7,
        color_discrete_map={
            "positive": "#2e7d32",
            "negative": "#d32f2f",
        },
        title="Распределение уверенности предсказания",
        labels={"confidence": "Confidence", "count": "Count"},
    )
    fig_conf.add_vline(
        x=0.85,
        line_dash="dash",
        line_color="orange",
        annotation_text="Alert threshold (0.85)",
    )
    st.plotly_chart(fig_conf, use_container_width=True)


# Блок 6: Последние алерты (негативные отзывы с высокой уверенностью) и топ товаров по сентименту

st.markdown("---")
st.subheader("Последние алерты (негатив с confidence ≥ 0.85)")

if ml_data:
    cols_alerts = [
        "timestamp", "asin",
        "title", "text", "confidence", "aspect_tags", "producer",
    ]

    alerts_src = df_ml[df_ml["is_alert"] == True]
    alerts_df = (
        alerts_src
        .tail(50)  # смотрим последние срабатывания
        .loc[:, [c for c in cols_alerts if c in alerts_src.columns]]
        .reset_index(drop=True)
    )

    if not alerts_df.empty:
        # Гарантируем наличие ожидаемых колонок
        for col in cols_alerts:
            if col not in alerts_df.columns:
                alerts_df[col] = ""

        # Преобразуем список аспектов в строку для табличного отображения
        if "aspect_tags" in alerts_df.columns:
            alerts_df["aspect_tags"] = alerts_df["aspect_tags"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else (x or "")
            )

        if "title" in alerts_df.columns:
            alerts_df["title"] = alerts_df["title"].fillna("").str.slice(0, 80)
        if "text" in alerts_df.columns:
            alerts_df["text"] = alerts_df["text"].fillna("").str.slice(0, 140)

        st.dataframe(alerts_df, use_container_width=True)
    else:
        st.success("Алертов пока нет 🎉")


# Блок 7: Топ товаров по сентименту (по последним событиям) и топ причин негатива (аспекты)

st.markdown("---")
st.subheader("Топ товаров по сентименту (по последним событиям)")

if ml_data:
    top_n = 10

    col_pos, col_neg = st.columns(2)

    with col_pos:
        st.markdown("**Топ позитивных товаров**")
        df_pos = df_ml[df_ml["predicted_label"] == "positive"]
        if not df_pos.empty:
            top_pos = (
                df_pos
                .groupby(["asin", "product_title"], as_index=False)
                .agg(
                    reviews=("predicted_label", "size"),
                    avg_confidence=("confidence", "mean"),
                )
                .sort_values(["reviews", "avg_confidence"], ascending=[False, False])
                .head(top_n)
            )
            top_pos["avg_confidence"] = top_pos["avg_confidence"].round(3)
            # Показываем только asin и метрики, product_title скрываем
            st.dataframe(
                top_pos[["asin", "reviews", "avg_confidence"]],
                use_container_width=True,
            )
            pos_img_path = "photos/invest_in_good.jpg"
            if os.path.exists(pos_img_path):
                st.image(
                    pos_img_path,
                    caption="Invest in good products",
                    use_column_width=True,
                )
        else:
            st.info("Пока нет позитивных предсказаний.")

    with col_neg:
        st.markdown("**Топ негативных товаров**")
        df_neg = df_ml[df_ml["predicted_label"] == "negative"]
        if not df_neg.empty:
            top_neg = (
                df_neg
                .groupby(["asin", "product_title"], as_index=False)
                .agg(
                    reviews=("predicted_label", "size"),
                    avg_confidence=("confidence", "mean"),
                )
                .sort_values(["reviews", "avg_confidence"], ascending=[False, False])
                .head(top_n)
            )
            top_neg["avg_confidence"] = top_neg["avg_confidence"].round(3)
            st.dataframe(
                top_neg[["asin", "reviews", "avg_confidence"]],
                use_container_width=True,
            )
            neg_img_path = "photos/invest_in_bad.jpg"
            if os.path.exists(neg_img_path):
                st.image(
                    neg_img_path,
                    caption="Invest in problem products",
                    use_column_width=True,
                )
        else:
            st.info("Пока нет негативных предсказаний.")


# Блок 8: Топ причин негатива (аспектный анализ) — для негативных отзывов с предсказанными аспектами

st.markdown("---")
st.subheader("Топ причин негатива (аспектный анализ, rule-based)")

if ml_data and "aspect_tags" in df_ml.columns:
    df_neg_aspects = df_ml[df_ml["predicted_label"] == "negative"].copy()

    # Разворачиваем список аспектов в строки
    df_neg_aspects = df_neg_aspects.explode("aspect_tags")
    df_neg_aspects = df_neg_aspects.dropna(subset=["aspect_tags"])

    if not df_neg_aspects.empty:
        aspect_counts = (
            df_neg_aspects["aspect_tags"]
            .value_counts()
            .reset_index()
        )
        aspect_counts.columns = ["aspect", "count"]
        aspect_counts = aspect_counts.head(10)

        fig_aspects = px.bar(
            aspect_counts,
            x="aspect",
            y="count",
            title="Топ аспектов в негативных отзывах",
            labels={"aspect": "Аспект", "count": "Количество негативных отзывов"},
        )
        st.plotly_chart(fig_aspects, use_container_width=True)
    else:
        st.info("Пока нет аспектов в негативных отзывах.")
else:
    st.info("Ожидаем данные по аспектам из ml-results...")
