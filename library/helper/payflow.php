<?php
  /**
   * do payment by payflow pro
   * 
   * @author wade
   */

  class Helper_Payflow
  {
	  /**
	   * host address for connection
	   * 'https://pilot-payflowpro.paypal.com' for testing purposes, 
	   * 'https://payflowpro.paypal.com' for live transactions
	   */
	  const HOST_ADDRESS = 'https://pilot-payflowpro.paypal.com';

	  /**
	   * host port for connection
	   */
	  const HOST_PORT = 443;

	  /**
	   * timeout for connection
	   * 30 seconds at least(recommended)
	   */
	  const TIMEOUT = 30;

	  /**
	   * do payment by curl
	   *
	   * @access public static
	   * @param $nvp array
	   * @return array
	   */
	  public static function doPayflow($nvp = array())
	  {
		  $nvp = self::arrayToStr($nvp);

		  // initialize curl
		  $curl = curl_init();

		  // curl settings
		  curl_setopt($curl , CURLOPT_HEADER , false);
		  curl_setopt($curl , CURLOPT_POST , true);
		  curl_setopt($curl , CURLOPT_RETURNTRANSFER , true);
		  curl_setopt($curl , CURLOPT_SSL_VERIFYPEER , false);
		  curl_setopt($curl , CURLOPT_TIMEOUT , self::TIMEOUT);
		  curl_setopt($curl , CURLOPT_POSTFIELDS , $nvp);
		  curl_setopt($curl , CURLOPT_PORT , self::HOST_PORT);
		  curl_setopt($curl , CURLOPT_URL, self::HOST_ADDRESS);
          
		  // if there are errors ,then throw exception
		  if (curl_errno($curl) != 0)
		  {
			  throw new Exception('curl operation failed:'.curl_error($curl));
		  }

		  // execute curl
		  $response = curl_exec($curl);
          
		  // if execute failed , then throw exception
		  if ($response === false)
		  {
			  throw new Exception('curl execute failed!');
		  }

		  // close curl
		  curl_close($curl);
          
		  // return 
		  return self::strToArray($response);
	  }

	  /**
	   * convert query string into associate array
	   * 
	   * @param $nvp string
	   * @access private static
	   * @return array
	   */
	  private static function strToArray($nvp)
	  {
		  $temp_array = array();
		  $nvp = explode('&' , trim($nvp , '&'));
		  foreach($nvp as $value)
		  {
              if (empty($value)) continue;
			  $temp_array2 = explode('=' , $value);
			  if (empty($temp_array2[1])) continue;
			  $temp_array[$temp_array2[0]] = $temp_array2[1];
		  }
		  return $temp_array;
	  }

	  /**
	   * convert associated array to query string
	   *
	   * @param $nvp array
	   * @access private static
	   * @return string
	   */
      private static function arrayToStr($nvp)
	  {
		  $temp_str = '';
		  if (!is_array($nvp)) return $nvp;
		  foreach ($nvp as $key => $value)
		  {
			  $temp_str .= $key.'='.$value.'&';
		  }
		  return rtrim($temp_str , '&');
	  }
  }
?>