<?php 

	class downloader {

		const DEBUG = false;

		/**
		* Downlaoded Class Constructor
		* @param 	Arguments 	$args 	It will contain all the arguments passed by the user without the first (Controller) and the second one (Action)
		* @return 	void
		*/
		function __construct($args = null) 
		{
			$this->args = $args;
			$this->db = helper::getPDOHandle();
			if (self::DEBUG) self::debugBlock();
		}

		/**
		* Action Download function: the function that allows crons and users to download the save resources in downloads folder
		* @param 	void
		* @return 	void
		*/
		public function actionDownload()
		{
			if (isset($this->args[0])) {
				$resource_name = $this->args[0];
				if ($resource_name == "all") {
					$resources = helper::getResources($this->db);
					foreach ($resources as $resource) {
						helper::writeLine("The resource ".$resource['local_name']." started downloading",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
						$result = self::downloadFile($resource);
						if ($result) {
							helper::writeLine("The resource ".$resource['local_name']." has been downloaded correctly",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
						}
						else {
							helper::writeLine("The resource ".$resource['local_name']." coudn't be downloaded",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
						}
					}
				}
				else {
					$resources = helper::getResources($this->db,$resource_name);
					if (!empty($resources)) {
						foreach ($resources as $resource) {
							helper::writeLine("The resource ".$resource['local_name']." started downloading",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							$downloaded = self::downloadFile($resource);
							if ($downloaded) {
								helper::writeLine("The resource ".$resource['local_name']." has been downlaoded to ".$downloaded,helper::SUCCESS_ALERT);
							}
							else {
								helper::writeLine("The resource ".$resource['local_name']." coudn't be downlaoded",helper::ERROR_ALERT);
							}
						}
					}
					else {
						helper::writeLine("Something went wrong: ".$resource_name." could be locked by another importation or it may not exists.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
					}
				}
			}
			else {
				$this->help();
			}
		}

		/**
		* Download one resource file
		* @param 	Resource 	$resource 	the MySql record file resource
		* @param 	Temp 		$is_temp 	if true saves the file in temps folder, otherwise saves in downloads folder
		* @return 	LocalPath 	$local_path the path of the downloaded file, false if something went wrong
		*/
		public static function downloadFile($resource, $is_temp = false) 
		{
			$retries = 0;

			do {
				
				switch ($resource['service']) {
					case 'ftp'	: $local_path = self::ftp($resource, $is_temp); break;
					case 'http' : $local_path = self::http($resource, $is_temp); break;
					case 'local': $local_path = self::local($resource, $is_temp); break;
					default 	: $local_path = false;
				}

				if (!$local_path) {
					$retries++;
				}

			} while (!$local_path && $retries<=10);

			return $local_path;
		}

		/**
		* Download one resource using FTP (File transfer protocol)
		* @param 	Resource 	$resource 	the MySql record file resource
		* @param 	Temp 		$is_temp 	if true saves the file in temps folder, otherwise saves in downloads folder
		* @return 	LocalPath 	$local_path the path of the downloaded file, false if something went wrong
		*/
		private function ftp($resource, $is_temp) 
		{
			$success = false;
			$ftp_handle = ftp_connect($resource['host']);

			$login = ftp_login($ftp_handle, $resource['username'], $resource['password']);

			if ($login) {
				ftp_pasv($ftp_handle, true);
				$file_name = helper::getFileNameFromPath($resource['path']);
				$local_path = $is_temp ? TEMPS.DS.$file_name : DOWNLOADS.DS.$file_name;
				$success = ftp_get($ftp_handle,$local_path,$resource['path'], FTP_BINARY);
				ftp_close($ftp_handle);

			}

			return $success ? $local_path : false;
		}

		/**
		* Download one resource using HTTP (Hyper text transfer protocol)
		* @param 	Resource 	$resource 	the MySql record file resource
		* @param 	Temp 		$is_temp 	if true saves the file in temps folder, otherwise saves in downloads folder
		* @return 	LocalPath 	$local_path the path of the downloaded file, false if something went wrong
		*/
		private function http($resource, $is_temp) 
		{
			$success = false;

			// if ($resource["local_name"] == "focelda_listino") {

			// 	$ch = curl_init();
			// 	$source = "http://www.focelda.info/csv/index.php/041177/041177.html";
			// 	curl_setopt($ch, CURLOPT_URL, $source);
			// 	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
			// 	$data = curl_exec ($ch);
			// 	curl_close ($ch);

			// 	$file_name = "041177.csv";
			// 	$local_path = "/opt/e_sync/downloads/041177.csv";
			// 	$file = fopen($local_path, "w+");
			// 	$success = fputs($file, $data);
			// 	fclose($file);

			// } else {

				$content = file_get_contents("http://".$resource['username'].":".$resource['password']."@".$resource['host'].DS.$resource['path']);

				if ($content) {
					$file_name = helper::getFileNameFromPath($resource['path']);
					$local_path = $is_temp ? TEMPS.DS.$file_name : DOWNLOADS.DS.$file_name;
					$success = file_put_contents($local_path,$content);
				}
			// }


			return $success ? $local_path : false;
		}


		/**
		* Download one resource locally
		* @param 	Resource 	$resource 	the MySql record file resource
		* @param 	Temp 		$is_temp 	if true saves the file in temps folder, otherwise saves in downloads folder
		* @return 	LocalPath 	$local_path the path of the downloaded file, false if something went wrong
		*/
		private function local($resource, $is_temp) 
		{
			$success = false;
			$content = file_get_contents($resource['path']);
			if ($content) {
				$file_name = helper::getFileNameFromPath($resource['path']);
				$local_path = $is_temp ? TEMPS.DS.$file_name : DOWNLOADS.DS.$file_name;
				$success = file_put_contents($local_path,$content);
			}
			return $success ? $local_path : false;
		}

		/**
		* Outputs help commands to the user
		*/
		public function help()
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			helper::writeLine(E_SYNC_ROOT." ".DOWNLOADER_ROOT." download all\t\tDownloads all active resources in ".DOWNLOADS);
			helper::writeLine(E_SYNC_ROOT." ".DOWNLOADER_ROOT." download [resource_name]\tDownloads [resource_name] if is active in ".DOWNLOADS);
			helper::writeLine(helper::getSeparator());
		}

		/**
		* Block method inserted in the class constructor to debug on the fly a method or a variable 
		* @param 	void
		* @return	void
		*/
		private function debugBlock() 
		{
			helper::writeLine("Debug Mode",helper::SYSTEM_ALERT);
			// insert here an output
			die();
		}
	}

?>