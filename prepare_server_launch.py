"""
    What this does basically is remove the resdue kafka files from the offline db(folders) and adfalcon db.
    TODO: upgrade python ver
    TODO: Maybe make it pretty ??
"""

from typing import List

# make these a cli argument
# like python <scriptname> -r [what to remove] -db to truncate database tables 
dirs_to_remove : List[str] = [
    r"D:\development\af-framework\ArabyAds.Framework.DistributedEventBroker\ArabyAds.Framework.DistributedEventBroker.Core\bin\Debug\net6.0\kafka.offline",
    r"D:\development\dsp-platform\src-netcore\ArabyAds.AdFalcon.Banker\ArabyAds.AdFalcon.Banker.Master\bin\Debug\net6.0\kafka.offline",
    r"D:\development\dsp-platform\src-netcore\ArabyAds.AdFalcon.Server.BiddingPrediction\ArabyAds.AdFalcon.Server.BiddingPrediction.Service.Host\bin\Debug\net6.0\kafka.offline",
    r"D:\development\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.MaintenanceJob\bin\Debug\net6.0\kafka.offline",
    r"D:\development\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.WebAPI\bin\Debug\net6.0\kafka.offline",
    r"G:\af-framework\ArabyAds.Framework.DistributedEventBroker\ArabyAds.Framework.DistributedEventBroker.Core\bin\Debug\net6.0\kafka.offline",
    r"G:\dsp-platform\src-netcore\ArabyAds.AdFalcon.Banker\ArabyAds.AdFalcon.Banker.Master\bin\Debug\net6.0\kafka.offline",
    r"G:\dsp-platform\src-netcore\ArabyAds.AdFalcon.Server.BiddingPrediction\ArabyAds.AdFalcon.Server.BiddingPrediction.Service.Host\bin\Debug\net6.0\kafka.offline",
    r"G:\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.MaintenanceJob\bin\Debug\net6.0\kafka.offline",
    r"G:\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.WebAPI\bin\Debug\net6.0\kafka.offline"


]

tables_to_truncate : List[str] = [
    "bc_billing_entity_spend_details",
    "MasterAccounts",
    "eventbroker_offset_Track"
]

db_con_string : str = "server=localhost;User Id=root;Password=root;database=adfalcon;Persist Security Info=True;Charset=utf8;Pooling=true;Min Pool Size=0;Max Pool Size=350;Connection Lifetime=300;Default Command Timeout=120;"
seperator = "***"

def remove_offline_db() -> None :
    import shutil
    """Removes the left over kafka db files as they are not needed in local development"""
    for folder in dirs_to_remove:
        try:    
            shutil.rmtree(folder)
            print("remaining kafka.offline files terminated *.* ")
        except Exception as ex:
            if type(ex).__name__ == "PermissionError":
                print("A service is running, stop it then try again.")
                continue
            if type(ex).__name__ == "FileNotFoundError":
                print("Folder already cleaned ~!")
                continue
            print(f"Couldn't remove cause: \n{type(ex).__name__}\n{folder}")
            continue

def truncate_tables() -> None:
    """Empties the tables relied upon for the service bus?? anyways they aren't needed"""
    from sqlalchemy import create_engine, text
    engine = create_engine("mysql+pymysql://root:root@localhost/adfalcon?charset=utf8")
    print(f"\n{seperator *15}\n")
    with engine.connect() as conn:
        for table in tables_to_truncate:
            try:
                "attempt to truncate ze table"
                conn.execute(f"TRUNCATE TABLE {table};")
                print(f"\"{table}\" truncated ~!")
            except Exception as ex:
               #! "oh no!"
               print(f"something died -> something: {ex}")
               #* "anyways"
               continue


if __name__ == "__main__":
    import threading
    jobs : List[threading.Thread] = [
        threading.Thread(target=remove_offline_db),
        threading.Thread(target=truncate_tables)
    ]
    for job in jobs: job.start()