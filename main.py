from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.services.mfi_validator.router import router as mfi_validator_router
from app.services.price_validator.router import router as price_validator_router
from app.services.market_monitor.router import router as market_monitor_router
from app.services.mfi_drafter.router import router as mfi_drafter_router

app = FastAPI(
    title="WFP Data Tools API",
    description="Backend API per validazione dati e generazione report WFP",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Next.js
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🚦 Smistamento ai 4 servizi
app.include_router(mfi_validator_router, prefix="/mfi-validator", tags=["MFI Validator"])
app.include_router(price_validator_router, prefix="/price-validator", tags=["Price Validator"])
app.include_router(market_monitor_router, prefix="/market-monitor", tags=["Market Monitor"])
app.include_router(mfi_drafter_router, prefix="/mfi-drafter", tags=["MFI Drafter"])

@app.get("/")
def root():
    return {
        "status": "ok",
        "services": [
            {"id": "mfi-validator", "name": "MFI Dataset Validator", "endpoint": "/mfi-validator/validate-file"},
            {"id": "price-validator", "name": "Price Data Validator", "endpoint": "/price-validator/validate-file"},
            {"id": "market-monitor", "name": "Market Monitor Generator", "endpoint": "/market-monitor/generate"},
            {"id": "mfi-drafter", "name": "MFI Report Generator", "endpoint": "/mfi-drafter/generate"}
        ]
    }

@app.get("/health")
def health():
    return {"status": "healthy"}