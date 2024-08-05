
# North Korea state news agency archive

## Project scope

This is an automated system designed to collect and archive news articles from the [Korean Central News Agency (KCNA)](http://www.kcna.kp), or 조선중앙통신. The project aims to maintain an up-to-date archive of news articles for topics of interest, such as political statements and world news. The collected data is stored in `JSON` format and is archived daily to an Amazon S3 bucket.

## Script Overview

The main script, `fetch_kcna.py`, performs the following tasks:

1. **Fetch menu links**: Retrieves the menu links for different news topics from the KCNA website (because they change and aren't semantic).
2. **Parse articles**: Collects the latest articles from each topic page, limiting to the first 5 articles per topic to reduce the number of requests.
3. **Convert dates**: Converts Juche dates to Gregorian dates for consistency in the archive.
4. **Fetch story text**: Retrieves the full text of each article for specific key topics.
5. **Update archive**: Combines the newly collected data with the existing archive, removes duplicates and updates the archive stored on S3.

### Proxy strategy

The script utilizes a proxy service to handle requests to the KCNA website. By setting `premium=true`, the script ensures that premium proxies are used, providing faster and more reliable performance from the service.  

### GitHub Actions

The project includes a GitHub Actions workflow that runs the script daily. The workflow checks out the repository, installs the necessary dependencies and executes the script. It then commits any changes to the archive and uploads the updated JSON files to an S3 bucket. (The proxy service is required here because KCNA apparently blocks the cloud infrastructure used by Github).

### Archiving to S3

The updated archive is stored in an Amazon S3 bucket at the following URL:

- [North Korea news archive](https://stilesdata.com/north-korea-news/headlines.json)

The archive began in August 2024, so it's limited but will grow over time each day, ensuring that the latest news articles are available for analysis and research.

## Sample Data Structure

The data is stored in JSON format with the following structure:

```json
{
    "topic": "Top News",
    "headline": "Press Statement of Vice Department Director of WPK Central Committee Kim Yo Jong",
    "link": "http://www.kcna.kp/en/article/q/134a0eb1839cb01381c703e99144182116b5c41fea8bd4fb75e6da9cf995125e.kcmsf",
    "date": "2024-07-14",
    "story_text": "Pyongyang, July 14 (KCNA) -- Kim Yo Jong, vice department director of the Central Committee of the Workers' Party of Korea, released the following press statement on Sunday afternoon:\nToday I was informed that dirty leaflets and things of the ROK scum have been found again in the border area and some deep areas of the Democratic People's Republic of Korea.\nSimilar information was continuously reported by party organizations, military and social organizations at all levels on Sunday morning.\nAccording to the information, the rubbishes were found in 17 places in Jangphung County of North Hwanghae Province and the area adjacent to it.\nUnits of the Korean People's Army, the Worker-Peasant Red Guards, public security and state security organs at all levels near the border are now making an all-out search, throwing into fire and disposing of the found rubbishes according to the regulation of dealing with enemy-dropped objects and providing against the possibility to find such things in addition.\nDespite the repeated warnings of the DPRK, the ROK scum are not stopping this crude and dirty play.\nAs already warned, the scum, who are resorting to do this play, will be more strongly criticized by their people.\nWe have fully introduced our countermeasure in such situation.\nThe ROK clans will be tired from suffering a bitter embarrassment and must be ready for paying a very high price for their dirty play. -0-"
}
```

## Getting Started

### Prerequisites

Ensure you have the following installed:

- Python 3.9 or later
- Required Python packages (specified in `requirements.txt`)

### Running locally

1. Clone the repository.
2. Set up your environment variables for AWS and the proxy service.
3. Run the script using:

   ```bash
   python fetch_kcna.py
   ```

### Setting Up GitHub Actions

The GitHub Actions workflow is configured in `.github/workflows/fetch_kcna.yml`. Ensure your repository secrets are set for AWS credentials and the proxy service key. 

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes or improvements.

## License

This project is licensed under the Creative Commons. 
