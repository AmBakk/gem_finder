# Hidden Gems: Player Performance & Value Insights

**Author**: Amine Bakkoury  

## Overview

The "Hidden Gems" project is an advanced analytical tool designed to uncover promising young football players (16-23 years old) who offer high performance at a relatively low market value. The analysis focuses on forward players, evaluating them based on both traditional metrics (such as goals and assists) and advanced metrics, including ambidexterity, height, contract expiration, and European competition experience.

This project leverages data scraped from [TransferMarkt](https://www.transfermarkt.com/) using the [transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets) package, which is processed and presented via an interactive dashboard to offer real-time insights.

### Weekly Updates

To ensure the analysis remains relevant as the season progresses and player performance fluctuates, the entire workflow (data scraping, processing, and dashboard updates) is **scheduled to run once a week**. This scheduling aligns with the assumption of **one matchday per week**, allowing the dashboard to evolve over time as new data becomes available.

## Data Sources

The core data for this project comes from TransferMarkt, a trusted source for player performance and market value information. This data is downloaded from Kaggle and processed using PySpark and Pandas.

### Key Datasets Used:
- `players.csv`: Contains basic player information such as date of birth, position, and market value.
- `appearances.csv`: Provides game-by-game player statistics, including goals, assists, and minutes played.
- `player_valuations.csv`: Includes the market valuation of players.
- `game_events.csv`: Details on specific in-game events like cards and substitutions.

## Performance Evaluation

We developed a **performance evaluation formula** to assess the hidden potential of young attackers:

$$
P = (G_{90} \times W_g) + (A_{90} \times W_a) + B_{amb} + B_{height} + B_{eu} + B_{cont} - (Y90 \times M_{yellow}) - (R90 \times M_{red})
$$

Where:
- $$G_{90}$$: Goals per 90 minutes  
- $$A_{90}$$: Assists per 90 minutes  
- $$W_g$$: Weight for goals (5)  
- $$W_a$$: Weight for assists (3)  
- $$B_{amb}$$: Ambidextrous bonus (2)  
- $$B_{height}$$: Height bonus for non-wingers >183 cm (1)  
- $$B_{eu}$$: European competition experience bonus (1)  
- $$B_{cont}$$: Contract expiration bonus (0.5)  
- $$Y_{90}$$: Yellow cards per 90 minutes (malus of -1 per card)  
- $$R_{90}$$: Red cards per 90 minutes (malus of -5 per card)

### Criteria for Consideration:
- Players must be between 16 and 23 years of age.
- Only attackers are considered.
- Players must have a career average of at least 450 minutes played per year.

## Interactive Dashboard

The final insights from the analysis are presented in an interactive Google Sheets dashboard, where users can filter players based on various parameters like **Age**, **Market Value**, **Position**, and **Height**. Additionally, the dashboard highlights players with expiring contracts, ambidexterity, and European competition experience, helping clubs identify undervalued talents.

Feel free to check out the **[interactive dashboard](https://lookerstudio.google.com/reporting/b96dc8b0-5f4f-477a-83e1-cba9a4c095b7/page/A?s=mayxjBkgQ1I)** to see the insights in action.


## Contact

For any queries or suggestions, feel free to reach out via [LinkedIn](https://linkedin.com/in/mohamedaminebakkoury) or email me at [mabakkoury@hotmail.com](mailto:mabakkoury@hotmail.com).


