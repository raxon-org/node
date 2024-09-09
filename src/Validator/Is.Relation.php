<?php
/**
 * @author          Remco van der Velde
 * @since           2020-09-18
 * @copyright       Remco van der Velde
 * @license         MIT
 * @version         1.0
 * @changeLog
 *     -            all
 */

use Raxon\Org\App;

use Raxon\Org\Module\Core;

use Raxon\Org\Exception\ObjectException;

/**
 * @throws ObjectException
 */
function validate_is_relation(App $object, $array=null, $field='', $argument='', $function=false): bool
{
    if(
        is_string($array) &&
        substr($array, 0, 1) === '[' &&
        substr($array, -1, 1) === ']'
    ){
        $array = Core::object($array, Core::OBJECT_ARRAY);
    }
    if(is_array($array)){
        foreach($array as $nr => $value){
            //format: *
            //format: %s%s-%s-%s-%s-%s%s%s
            if($value === '*' && count($array) === 1){
                return true;
            }
            if(strlen($value) !== 36){
                return false;
            }
            $explode = explode('-', $value);
            if(count($explode) !== 5){
                return false;
            }
            if(strlen($explode[0]) !== 8){
                return false;
            }
            if(strlen($explode[1]) !== 4){
                return false;
            }
            if(strlen($explode[2]) !== 4){
                return false;
            }
            if(strlen($explode[3]) !== 4){
                return false;
            }
            if(strlen($explode[4]) !== 12){
                return false;
            }
        }
        return true;
    }
    elseif($array === '*'){
        return true;
    }
    return false;
}
