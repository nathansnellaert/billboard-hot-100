# Billboard Hot 100 Charts

Complete historical dataset of Billboard Hot 100 weekly charts from 1958 to present.

## About

The Billboard Hot 100 is the music industry standard record chart in the United States for songs, published weekly by Billboard magazine. Chart rankings are based on sales (physical and digital), radio play, and online streaming in the United States.

## Data Source

- **Original Source**: Billboard.com
- **Aggregator**: GitHub repository (mhollingshead/billboard-hot-100)
- **License**: Open Data (Public GitHub repository)
- **URL**: https://github.com/mhollingshead/billboard-hot-100
- **Updates**: Daily automated updates

## Dataset

### hot_100_charts
Complete Billboard Hot 100 chart history (1958-2025)

**Columns:**
- `chart_date` - Week of the chart (YYYY-MM-DD format, Saturdays)
- `rank` - Position on the chart (1-100)
- `song` - Song title
- `artist` - Artist name(s)
- `last_week` - Previous week's position (NULL for new entries)
- `peak_position` - Highest position ever reached by this song
- `weeks_on_chart` - Total weeks this song has been on the chart

**Time Range**: August 4, 1958 - November 29, 2025

**Record Count**: ~351,000+ chart entries across 3,513 weekly charts

## Key Statistics

- **Total Songs**: 26,670 unique songs
- **Total Artists**: 11,158 unique artists
- **Total Charts**: 3,513 weeks (67+ years)
- **#1 Hits**: 1,157 different songs reached #1

## Notable Facts

- **Most Weeks on Chart**: Taylor Swift (19,523 cumulative weeks)
- **Longest Running Chart**: 67+ years of continuous weekly data
- **Latest Update**: November 29, 2025

## Data Quality

- Complete weekly coverage from inception (Aug 1958)
- Automated daily updates from Billboard
- Includes historical chart movement data (peak position, weeks on chart)
- NULL values for `last_week` indicate new chart entries

## Use Cases

- Music popularity trend analysis
- Artist success metrics
- Song longevity analysis
- Genre evolution studies
- Predictive modeling for chart success
- Music recommendation systems
- Cultural trend analysis

## Query Examples

**Top 10 Current Week:**
```sql
SELECT rank, song, artist, weeks_on_chart
FROM hot_100_charts
WHERE chart_date = (SELECT MAX(chart_date) FROM hot_100_charts)
ORDER BY rank
LIMIT 10
```

**All #1 Hits:**
```sql
SELECT DISTINCT song, artist, peak_position, MAX(weeks_on_chart) as weeks
FROM hot_100_charts
WHERE rank = 1
GROUP BY song, artist, peak_position
ORDER BY weeks DESC
```

**Artist Chart Performance:**
```sql
SELECT artist,
       COUNT(DISTINCT song) as total_songs,
       SUM(weeks_on_chart) as total_weeks,
       COUNT(CASE WHEN rank = 1 THEN 1 END) as number_ones
FROM hot_100_charts
GROUP BY artist
ORDER BY total_weeks DESC
LIMIT 20
```

## Citation

Data aggregated from Billboard Hot 100 charts via the mhollingshead/billboard-hot-100 GitHub repository.
