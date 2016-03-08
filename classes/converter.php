<?php 

	class converter {

		const DEBUG = false;
		const DELETE_ORIGINAL_AFTER_CONVERSION = false;

		/**
		* Converter Class Constructor
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
		* Action Converter function: the function that allows crons and users to convert the saved resources in downloads folder
		* @param 	void
		* @return 	void
		*/
		public function actionConvert() 
		{
			$query = $this->db->prepare("SELECT * FROM system_resources 
										 LEFT JOIN system_sources ON (system_resources.source_id = system_sources.id)
										 LEFT JOIN system_services ON (system_sources.service_id = system_services.id)
										 WHERE active = 1 AND conversion_rule != 'csv';");
			$query->execute();
			$resources = $query->fetchAll();
			foreach ($resources as $resource) {
				$converted = self::convertFile($resource);
				if ($converted) {
					helper::writeLine("The resource ".$resource['local_name']." has been converted to ".$converted,helper::SUCCESS_ALERT);
				}
				else {
					helper::writeLine("The resource ".$resource['local_name']." coudn't be converted",helper::ERROR_ALERT);
				}
			}
		}

		/**
		* Convert one resource file
		* @param 	Resource 	$resource 	the MySql record file resource
		* @param 	Temp 		$is_temp 	if true saves the file in temps folder, otherwise saves in downloads folder
		* @return 	LocalPath 	$local_path the path of the downloaded file, false if something went wrong
		*/
		public function convertFile($resource, $is_temp = false)
		{
			$local_path = false;

			$conversions = explode(helper::ARROW,$resource['conversion_rule']);
			$file_name = helper::getFileNameFromPath($resource['path']);
			$local_path = $is_temp ? TEMPS.DS.$file_name : DOWNLOADS.DS.$file_name;
			$exists = file_exists($local_path);

			if ($exists) {

				for ($i=0; $i<count($conversions); $i++) {
					
					$old_local_path = $local_path;

					switch ($conversions[$i]) {
						case 'zip' 	: $local_path = self::decompressZip($local_path); break;
						case 'gz'	: $local_path = self::decompressGzip($local_path); break;
						case 'xls' 	: $local_path = self::convertXls($local_path); break;
					}

					if (self::DELETE_ORIGINAL_AFTER_CONVERSION && $i < count($conversions)-1 && $local_path) 
						unlink($old_local_path);
				}
			}

			return $exists ? $local_path : false;
		}

		/**
		* Decompress a zip resource file
		* @param 	Local Path 	$path 		The local path of the resource to decompress
		* @return 	Local Path 	$new_path 	the path of the decompressed file, false if something went wrong
		*/
		private function decompressZip($path) 
		{	
			$new_path = false;
			if ($path) {
				$dir = helper::getDirFromFilePath($path);
				$zip = new ZipArchive;

				$file = $zip->open($path);

				if ($file === true) {
					
					$extracted = $zip->extractTo($dir);
					$temp = $zip->statIndex(0);
					$file_name = $temp['name'];
					$new_path = $dir.DS.$file_name;

					$zip->close();
				}
			}
			return $new_path;
		}
		
		/**
		* Decompress a gzip resource file
		* @param 	Local Path 	$path 		The local path of the resource to decompress
		* @return 	Local Path 	$new_path 	the path of the decompressed file, false if something went wrong
		*/
		static function decompressGzip($path) 
		{
			$new_path = false;
			if ($path) {
				$buffer_size = 4096;
				$gz_handle = @gzopen($path,'rb');
				if ($gz_handle) {
					$new_path = str_replace(".gz","",$path);
					$handle = fopen($new_path, 'wb'); 
					if ($handle) {
						while (!gzeof($gz_handle)) {
							fwrite($handle, gzread($gz_handle, $buffer_size));
						}
					}
				}
			}
			return ($path && $gz_handle && $handle) ? $new_path : false;
		}

		/**
		* Convert an Excel resource file
		* @param 	Local Path 	$path 		The local path of the resource to decompress
		* @return 	Local Path 	$new_path 	the path of the converted file, false if something went wrong
		*/
		static function convertXls($path) {

			$new_path = false;
			if ($path) {
				try {
					require_once 'PHPExcel/Classes/PHPExcel/IOFactory.php';

					$file_name = helper::getFileNameFromPath($path);
					$extension = helper::getFileExtensionFromFileName($file_name);
					$version = $extension == 'xlsx' ? 'Excel2007' : 'Excel5';
					$reader = PHPExcel_IOFactory::createReader($version);
					$excel = $reader->load($path);

					if ($excel)  {
						$writer = PHPExcel_IOFactory::createWriter($excel, 'CSV');
						$new_path = str_replace($extension,"csv",$path);
						$writer->save($new_path);
					}
				} 
				catch (Exception $e) {
					helper::writeLine($e->getMessage(),helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
					helper::writeLine("Are you sure the file is not a fake Excel file? maybe a CSV file with a changed extension?",helper::SYSTEM_ALERT,helper::DIE_AFTER_MSG);
				}

			}
			return $new_path;
		}

		/**
		* Outputs help commands to the user
		*/
		public function help()
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			helper::writeLine(E_SYNC_ROOT." ".CONVERTER_ROOT." convert\t\tConverts all files in ".DOWNLOADS);
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