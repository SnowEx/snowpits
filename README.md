# snowpits repo
This repository is home to the NASA SnowEx Snow Pit processing scripts, blank field templates, and electronic data entry sheets. Additionaly, there is a 'general_use' directory that is intended for snow school or other field campaign applications. In the directory you'll find printable data sheets, electronic field entry forms (excel macros sheets with drop down options in some fields to maintain consistancy in data transfering, and a general purpose script to process snow pit parameters (i.e. density, temperature, liquid water content, and stratigrapy) and summary files (i.e. SWE and environment/wx conditions).  

# organization 
There are four main directories organized by SnowEx field campaign year: SnowEx2017, SnowEx2020, SnowEx2021, and SnowEx2023, hereinafter referred to as S17, S20, S21, and S23, with an additional directory for general use (i.e. the script is more flexible, fluent, and not catered to nuances for each NASA SnowEx field campaign).

Within each directory there is the main script that produces the parameter files (_density.csv, _temperature.csv, _lwc.csv, _stratigraphy.csv, etc) from the electronic snow pit sheets. 

# SnowEx (brief) background: 
| Year | Campaign Type | Measurement Focus |
|------|---------------|--------------------|
| 2017 | IOP           | Colorado, focused on multiple instruments in a forest gradient. |
| 2020 | IOP, TS       | Western U.S focused on Time Series of L-band InSAR, active/passive microwave for SWE and thermal IR for snow surface temp. |
| 2021 | TS            | Western U.S, continued Time Series of L-band InSAR, also addressed prairie & snow albedo questions. |
| 2023 | IOP           | Alaska Tundra & Boreal forest, focused on addressing SWE/snow depth and albedo objectives. |

*IOP = Intense Observation Period (~2-3 week, daily observations); TS = Time Series (~3-5 month winter, weekly observations)*

