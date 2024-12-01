# Trading Project Organization Guidelines
Last Updated: November 21, 2024

## I. Directory Structure

### Root Directory
```
trading-project/
├── data/
├── analysis/
├── research/
└── docs/
```

### Detailed Structure
```
trading-project/
├── data/
│   ├── market-data/
│   │   ├── options-chains/
│   │   ├── economic-calendar/
│   │   └── dark-pool/
│   └── earnings/
│
├── analysis/
│   ├── daily-updates/
│   │   ├── market-updates/
│   │   └── ticker-updates/
│   └── trading-plans/
│       ├── master-plan.md
│       └── strategies/
│
├── research/
│   ├── analysts/
│   └── articles/
│
└── docs/
    └── instructions.md
```

## II. File Naming Conventions

### 1. Date-Based Files
- Format: `YYYY-MM-DD-description.extension`
- Example: `2024-11-21-options-chain.csv`
- Use hyphens (-) for separators
- All lowercase for descriptions
- No spaces in filenames

### 2. Standard Files
- Use kebab-case for filenames
- Example: `master-trading-plan.md`
- Include version numbers if needed: `trading-rules-v2.md`

## III. File Organization Guidelines

### A. Market Data Files
1. **Options Chains**
   - Daily files in `data/market-data/options-chains/`
   - Include ticker list in filename for focused reports
   - Example: `2024-11-21-tech-options-chain.csv`

2. **Economic Calendar**
   - Store in `data/market-data/economic-calendar/`
   - Weekly files preferred
   - Example: `2024-W47-economic-calendar.csv`

3. **Dark Pool Data**
   - Daily files in `data/market-data/dark-pool/`
   - Include timestamp in filename
   - Example: `2024-11-21-1600-dark-pool.csv`

### B. Analysis Files
1. **Daily Updates**
   - Separate market and ticker updates
   - Include timestamp for intraday updates
   - Example: `2024-11-21-1400-market-update.md`

2. **Trading Plans**
   - Master plan in root of trading-plans/
   - Individual strategies in strategies/
   - Update timestamps in file headers

### C. Research Files
1. **Analyst Content**
   - Organize by analyst name
   - Include source and date
   - Example: `research/analysts/felix-prehn/2024-11-nvda-analysis.md`

2. **Articles**
   - Include source in filename
   - Example: `2024-11-21-yahoo-finance-nvda-earnings.md`

## IV. File Content Standards

### A. Markdown Files
1. **Headers**
```markdown
# Title
Last Updated: [Date]
Author/Source: [Name]

## Table of Contents
```

2. **Trading Plans**
```markdown
# Strategy Name
Updated: [Date] [Time] [Timezone]

## I. SETUP CONDITIONS
## II. POSITION SIZING
## III. ENTRY RULES
## IV. MANAGEMENT RULES
## V. EXIT SCENARIOS
```

### B. Data Files
1. **CSV Headers**
   - Consistent column names
   - Include units where applicable
   - Add data dictionary if needed

2. **Date Formats**
   - Use ISO 8601: YYYY-MM-DD
   - Include timezone for timestamps
   - Example: 2024-11-21T14:00:00 CET

## V. Maintenance Guidelines

### A. Daily Tasks
1. Archive previous day's data files
2. Update daily analysis files
3. Verify file organization compliance

### B. Weekly Tasks
1. Consolidate daily updates
2. Review and clean unused files
3. Update master trading plan

### C. Monthly Tasks
1. Archive old data files
2. Review and update documentation
3. Validate file structure

## VI. Automation Recommendations

### A. File Organization Scripts
1. Automated file sorting based on naming conventions
2. Data validation for CSV files
3. Backup creation for critical files

### B. Update Tracking
1. Version control for trading plans
2. Change logs for strategy updates
3. Automatic timestamp updates

## VII. Future Improvements

### A. Planned Enhancements
1. Integration with trading platforms
2. Automated data collection
3. Performance tracking system

### B. Documentation Needs
1. Strategy performance tracking
2. Risk management logs
3. Trade execution records

Would you like to:
1. Add more specific content templates?
2. Include automation script examples?
3. Add additional organization categories?