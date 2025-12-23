CREATE TABLE IF NOT EXISTS Options_Greeks_By_Strike (
    asset TEXT,
    date TEXT,
    strike REAL,
    call_oi REAL,
    put_oi REAL,
    net_oi REAL,
    call_gex REAL,
    put_gex REAL,
    net_gex REAL,
    call_dex REAL,
    put_dex REAL,
    net_dex REAL,
    PRIMARY KEY (asset, date, strike)
);

CREATE TABLE IF NOT EXISTS Options_Greeks_By_Strike_Cumulative (
    asset TEXT,
    date TEXT,
    strike REAL,
    call_oi REAL,
    put_oi REAL,
    net_oi REAL,
    call_gex REAL,
    put_gex REAL,
    net_gex REAL,
    call_dex REAL,
    put_dex REAL,
    net_dex REAL,
    PRIMARY KEY (asset, date, strike)
);

CREATE TABLE IF NOT EXISTS Options_Greeks_By_Expiry (
    asset TEXT,
    expiry_date TEXT,
    dte INTEGER,
    contract_count INTEGER,
    call_oi REAL,
    put_oi REAL,
    net_oi REAL,
    call_gamma REAL,
    put_gamma REAL,
    total_gamma REAL,
    call_gex REAL,
    put_gex REAL,
    net_gex REAL,
    call_dex REAL,
    put_dex REAL,
    net_dex REAL,
    PRIMARY KEY (asset, expiry_date)
);

CREATE TABLE IF NOT EXISTS Options_Greeks_By_Strike_All_Expiries (
    asset TEXT,
    strike REAL,
    call_oi REAL,
    put_oi REAL,
    net_oi REAL,
    call_gex REAL,
    put_gex REAL,
    net_gex REAL,
    call_dex REAL,
    put_dex REAL,
    net_dex REAL,
    PRIMARY KEY (asset, strike)
);
