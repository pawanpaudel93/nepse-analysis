from nepse import NEPSE


if __name__ == "__main__":
    nepse = NEPSE()
    # display sectors
    nepse.display_sectors()
    # display securities
    nepse.display_securities()
    # display floorsheet for HIDCL
    nepse.display_floorsheet(symbol="HIDCL", date="2021-08-01")
    # display floorsheet for sector_id 52, top 5 brokers
    nepse.display_sector_floorsheet(52, top_n=5, date="2021-08-01")
