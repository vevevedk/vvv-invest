# Trading Data File Standards
Last Updated: November 21, 2024

## I. OPTIONS CHAIN DATA
```csv
Updated_Date,Hour,Ticker,Expiration_Date,Call_Put,Strike,Bid,Ask,Last,Volume,OI,IV,Delta,Gamma,Theta,Vega,Premium_USD,Type
2024-11-21,14:00,SPY,2024-12-20,C,500,2.45,2.48,2.46,1250,5000,0.25,0.45,0.02,-0.03,0.15,24500,Weekly
```

### Required Fields:
1. **Updated_Date**: YYYY-MM-DD
2. **Hour**: 24-hour format (HH:MM)
3. **Ticker**: Stock symbol
4. **Expiration_Date**: YYYY-MM-DD
5. **Call_Put**: C or P
6. **Strike**: Decimal to 2 places
7. **Bid/Ask/Last**: Decimal to 2 places
8. **Volume**: Integer
9. **OI**: Open Interest (Integer)
10. **Greeks**: 2 decimal places
11. **Premium_USD**: Contract value in USD
12. **Type**: Weekly/Monthly/LEAPS

## II. DARK POOL DATA
```csv
Updated_Date,Hour,Ticker,Trade_Type,Sentiment,Stock_Price,Strike,Probability,Premium_USD,Time_Since,Exp_Type,Trade_ID,Clean_Premium,DTE
2024-11-21,14:00,NVDA,Buy,Bullish,500.00,520,0.75,250000,2h,Weekly,NVDA_B_520C_1121,250000,30
```

### Required Fields:
1. **Updated_Date/Hour**: As above
2. **Trade_Type**: Buy/Sell
3. **Sentiment**: Bullish/Bearish/Neutral
4. **Stock_Price**: Current price
5. **Premium_USD**: Trade size in USD
6. **Time_Since**: Time since trade
7. **Trade_ID**: Unique identifier
8. **DTE**: Days to expiration

## III. ECONOMIC CALENDAR
```csv
Updated_Date,Event_Date,Time,Country,Event_Type,Actual,Previous,Consensus,Forecast,Impact
2024-11-21,2024-11-22,14:30,US,CPI MoM,0.3%,0.2%,0.3%,0.3%,High
```

### Required Fields:
1. **Event_Date**: YYYY-MM-DD
2. **Time**: Local time (24H)
3. **Country**: Country code
4. **Event_Type**: Description
5. **Values**: % or absolute
6. **Impact**: High/Medium/Low

## IV. EARNINGS CALENDAR
```csv
Updated_Date,Report_Date,Time,Ticker,Company_Name,EPS_Est,EPS_Prev,Rev_Est,Rev_Prev,Market_Cap,Quarter,Time_Frame
2024-11-21,2024-11-22,BMO,NVDA,NVIDIA Corp,3.37,2.70,16.19B,13.51B,1.2T,Q3,Pre-Market
```

### Required Fields:
1. **Report_Date**: YYYY-MM-DD
2. **Time**: BMO/AMC (Before/After Market)
3. **Ticker/Company**: Full details
4. **Estimates**: Current/Previous
5. **Market_Cap**: In billions/trillions
6. **Time_Frame**: Pre/Post/During Market

## V. MARKET UPDATES
```csv
Updated_Date,Hour,Source,Subject,Category,Commentary,Impact,Action_Required
2024-11-21,14:00,Fed,Powell Speech,Monetary Policy,"Hawkish comments on rates",High,Review hedges
```

### Required Fields:
1. **Source**: Origin of update
2. **Subject**: Brief description
3. **Category**: Type of update
4. **Commentary**: Detailed notes
5. **Impact**: High/Medium/Low
6. **Action_Required**: Required changes

## VI. TICKER UPDATES
```csv
Updated_Date,Hour,Ticker,Update_Type,Price_Change,Volume_Change,Options_Flow,Technical_Level,Action_Required
2024-11-21,14:00,AAPL,Technical,-2.3%,+150%,Bearish Put Sweep,$180 Support,Monitor position
```

### Required Fields:
1. **Ticker**: Stock symbol
2. **Update_Type**: Technical/Flow/News
3. **Changes**: Price/Volume changes
4. **Options_Flow**: Notable options activity
5. **Technical_Level**: Key price levels
6. **Action_Required**: Needed adjustments

## VII. DATA VALIDATION RULES

### A. General Rules
1. No empty fields allowed
2. Dates must be valid
3. Percentages must include % symbol
4. Monetary values must be clean numbers

### B. Numbers Format
1. Prices: 2 decimal places
2. Percentages: 2 decimal places
3. Large numbers: Use K/M/B/T suffix
4. Times: 24-hour format

### C. Text Fields
1. Use consistent abbreviations
2. No special characters
3. Use standard separators
4. Case sensitive where noted

## VIII. FILE NAMING CONVENTION
```
YYYY-MM-DD-HH00-category-description.csv
```

Example: `2024-11-21-1400-options-chain-tech.csv`

## IX. AUTOMATION NOTES

### A. Data Validation Scripts
1. Check field formats
2. Verify calculations
3. Flag unusual values

### B. File Processing
1. Auto-date stamping
2. Field standardization
3. Error logging

Would you like me to:
1. Add specific validation rules for any category?
2. Create example data processing scripts?
3. Add more data categories or fields?