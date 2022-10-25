"""
    What this does basically is remove the resdue kafka files from the offline db(folders) and adfalcon db.
    TODO: upgrade python ver
    TODO: Maybe make it pretty ??
"""

from time import sleep
from typing import List
import threading
# make these a cli argument
# like python <scriptname> -r [what to remove] -db to truncate database tables 
dirs_to_remove : List[str] = [
    # r"D:\development\af-framework\ArabyAds.Framework.DistributedEventBroker\ArabyAds.Framework.DistributedEventBroker.Core\bin\Debug\net6.0\kafka.offline",
    # r"D:\development\dsp-platform\src-netcore\ArabyAds.AdFalcon.Banker\ArabyAds.AdFalcon.Banker.Master\bin\Debug\net6.0\kafka.offline",
    # r"D:\development\dsp-platform\src-netcore\ArabyAds.AdFalcon.Server.BiddingPrediction\ArabyAds.AdFalcon.Server.BiddingPrediction.Service.Host\bin\Debug\net6.0\kafka.offline",
    # r"D:\development\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.MaintenanceJob\bin\Debug\net6.0\kafka.offline",
    # r"D:\development\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.WebAPI\bin\Debug\net6.0\kafka.offline",
   
    r"E:\development\arabyads_work\af-framework\ArabyAds.Framework.DistributedEventBroker\ArabyAds.Framework.DistributedEventBroker.Core\bin\Debug\net6.0\kafka.offline",
    r"E:\development\arabyads_work\dsp-platform\src-netcore\ArabyAds.AdFalcon.Banker\ArabyAds.AdFalcon.Banker.Master\bin\Debug\net6.0\kafka.offline",
    r"E:\development\arabyads_work\dsp-platform\src-netcore\ArabyAds.AdFalcon.Server.BiddingPrediction\ArabyAds.AdFalcon.Server.BiddingPrediction.Service.Host\bin\Debug\net6.0\kafka.offline",
    r"E:\development\arabyads_work\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.MaintenanceJob\bin\Debug\net6.0\kafka.offline",
    r"E:\development\arabyads_work\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.WebAPI\bin\Debug\net6.0\kafka.offline",
]
programs_to_run : List[str] = [

    r"E:\development\arabyads_Work\af-framework\ArabyAds.Framework.DistributedEventBroker\ArabyAds.Framework.DistributedEventBroker.Core\bin\Debug\net6.0\ArabyAds.Framework.DistributedEventBroker.Core.exe",
    r"E:\development\arabyads_Work\dsp-platform\src-netcore\ArabyAds.AdFalcon.Banker\ArabyAds.AdFalcon.Banker.Master\bin\Debug\net6.0\ArabyAds.AdFalcon.Banker.Master.exe",
    r"E:\development\arabyads_Work\dsp-platform\src-netcore\ArabyAds.AdFalcon.Server.BiddingPrediction\ArabyAds.AdFalcon.Server.BiddingPrediction.Service.Host\bin\Debug\net6.0\ArabyAds.AdFalcon.Server.BiddingPrediction.Service.Host.exe",
    r"E:\development\arabyads_Work\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.MaintenanceJob\bin\Debug\net6.0\ArabyAds.AdFalcon.Server.MaintenanceJob.exe",
    r"E:\development\arabyads_Work\dsp-platform\src-netcore\AdFalcon.AdServer\ArabyAds.AdFalcon.Server.WebAPI\bin\Debug\net6.0\ArabyAds.AdFalcon.Server.WebAPI.exe",
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
    from sqlalchemy import create_engine
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
    print(f"\n{seperator *15}\n")
    

def run_apps()-> None:
    """Runs the apps defined in the apps list"""

    import subprocess
    for program in programs_to_run:
        app_name= program.split('\\')[-1]
        print(f"opening:\t{app_name}")
        sleep(1)
        subprocess.Popen(program,creationflags=subprocess.CREATE_NEW_CONSOLE )

def main():
    jobs : List[threading.Thread] = [
        threading.Thread(target=remove_offline_db),
        threading.Thread(target=truncate_tables),
        threading.Thread(target=run_apps)
    ]
    for job in jobs:  job.start()



if __name__ == "__main__":
    main()


    