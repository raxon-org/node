<?php
namespace Raxon\Node\Service;

use Raxon\App;
use Raxon\Module\Controller;
use Raxon\Module\Data;

use Exception;

class Security extends Main
{

    /**
     * @throws Exception
     */
    public static function is_granted($class, $role, $options): bool
    {
        if(!array_key_exists('function', $options)){
            throw new Exception('Function is missing in options');
        }
        $name = Controller::name($class);
        $name_permission = str_replace('.', ':', $name);
        $function_permission = str_replace('_', '.', $options['function']);
        $role = new Data($role);
        $is_permission = false;
        $is_permission_relation = false;
        $is_permission_parse = false;
        $permissions = [];
        $permissions[] = $name_permission . ':' . $function_permission;
        if(
            array_key_exists('relation', $options) &&
            $options['relation'] === true
        ){
            $permissions[] = $name_permission . ':' . $function_permission . '.' . 'relation';
        }
        if(
            array_key_exists('parse', $options) &&
            $options['parse'] === true
        ){
            $permissions[] = $name_permission . ':' . $function_permission . '.' . 'parse';
        }
        if(array_key_exists('debug', $options)){
            d(get_class($role));
            ddd(get_class_methods($role));
        }
        if(method_exists($role, 'getPermissions')){
            $role_permissions = (array) $role->getPermissions();
            foreach ($role_permissions as $nr => $role_permission){
                ddd($role_permission);
            }

        } else {
            $role_permissions = $role->get('permission');
        }
        if(is_array($role_permissions)){
            foreach($role_permissions as $permission){
                $permission = new Data($permission);
//                d($permission);
                if($permission->get('name') === $name_permission . ':' .$function_permission){
                    $is_permission = true;
                }
                if(
                    array_key_exists('relation', $options) &&
                    $options['relation'] === true
                ){
                    if($permission->get('name') === $name_permission . ':' .$function_permission . '.' . 'relation'){
                        $is_permission_relation = true;
                    }
                } else {
                    $is_permission_relation = true;
                }
                if(
                    array_key_exists('parse', $options) &&
                    $options['parse'] === true
                ) {
                    if($permission->get('name') === $name_permission . ':' . $function_permission . '.' . 'parse'){
                        $is_permission_parse = true;
                    }
                } else {
                    $is_permission_parse = true;
                }
                if(
                    $is_permission === true &&
                    $is_permission_parse === true &&
                    $is_permission_relation === true
                ){
                    return true;
                }
            }
        }
        throw new Exception('Security: permission denied... (' . implode(', ', $permissions) . ')');
    }
}