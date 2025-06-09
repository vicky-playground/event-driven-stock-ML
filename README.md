# ⚡ Event-Driven Stock Prediction

This project is a hands-on learning exercise to explore:

- **Event-Driven Architecture (EDA)**
- **Command Query Responsibility Segregation (CQRS)**
- **Event Sourcing**
- **Apache Kafka**
- Real-time ML with **LSTM models**

It demonstrates how real-time events can drive stock prediction workflows in a scalable, loosely coupled system.

👉 **Read the full article on the benefits of Event-Driven Architecture here:**  
[🔗 How Event-Driven Architecture Transforms AI/ML](https://vicky-note.medium.com/how-event-driven-architecture-transforms-ai-ml-ab5d47d8a745)

⚠️ Please note the prediction is not reliable at all as the LSTM model does not account for critical stock market factors such as company fundamentals, macroeconomic indicators, or market sentiment.

![alt text](image.png)
---

## 📚 Key Concepts

- **CQRS**: Separate models for commands (writes) and queries (reads)
- **Event Sourcing**: All actions recorded as immutable events
- **Kafka**: Event broker to decouple producers and consumers
- **ML Pipeline**: LSTM model with technical indicators
- **Dashboard**: Web interface to send commands and visualize results

---

## 🧱 Architecture
User → HTTP API → Command → Kafka Events → Async ML → Predictions → Read Model 
![image](https://github.com/user-attachments/assets/1ffef757-f527-4f20-ad48-13283b33b1ab)


---

## 🚀 Quick Start

### 1. Clone the Repo

```bash
git clone https://github.com/your-username/event-driven-stock-prediction.git
cd event-driven-stock-prediction
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Add Environment Variables
Create a .env file:
```
FINNHUB_API_KEY=your_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
FLASK_ENV=development
```

### 4. Start the App
```bash
python app.py

```




