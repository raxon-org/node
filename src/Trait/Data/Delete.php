<?php

namespace Raxon\Node\Trait\Data;

use Raxon\App;
use Raxon\Config;

use Raxon\Module\Controller;
use Raxon\Module\Core;
use Raxon\Module\Event;
use Raxon\Module\File;

use Raxon\Node\Service\Security;

use Exception;

trait Delete {

    /**
     * @throws Exception
     */
    public function delete($class, $role, $options=[]): bool
    {
        $name = Controller::name($class);
        $options = Core::object($options, Core::OBJECT_ARRAY);
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        if(!array_key_exists('uuid', $options)){
            return false;
        }
        if(!Core::is_uuid($options['uuid'])){
            return false;
        }
        $options['relation'] = false;
        if(!Security::is_granted(
            $name,
            $role,
            $options
        )){
            return false;
        }
        $object = $this->object();
        $dir_data = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds')
        ;
        $url = $dir_data . $name . $object->config('extension.json');
        $data = $object->data_read($url);
        if(!$data){
            return false;
        }
        $list = $data->data($name);
        if(!$list){
            return false;
        }
        $result = false;
        foreach($list as $nr => $record){
            if(
                is_object($record) &&
                property_exists($record, 'uuid')
            ){
                if($record->uuid === $options['uuid']){
                    if(is_array($list)){
                        unset($list[$nr]);
                        $result = true;
                    }
                    elseif(is_object($list)){
                        unset($list->{$nr});
                        $result = true;
                    }
                    if(empty($list)){
                        File::delete($url);
                    } else {
                        if(is_object($list)){
                            $list = (array) $list;
                        }
                        $list = array_values($list);
                        $data->data($name, $list);
                        $data->write($url);
                    }
                    return $result;
                }
            }
        }
        return false;
    }

    /**
     * @throws Exception
     */
    public function delete_many($class, $role, $options=[]): array
    {
        $result = [];
        $name = Controller::name($class);
        $options = Core::object($options, Core::OBJECT_ARRAY);
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        if(!array_key_exists('uuid', $options)){
            return $result;
        }
        $uuid = $options['uuid'];
        if(!is_array($uuid)){
            return $result;
        }
        foreach($uuid as $nr => $value){
            if(!Core::is_uuid($value)){
                return $result;
            }
        }
        $options['relation'] = false;
        if(!Security::is_granted(
            $name,
            $role,
            $options
        )){
            return $result;
        }
        $object = $this->object();
        $dir_data = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds')
        ;
        $url = $dir_data . $name . $object->config('extension.json');
        $data = $object->data_read($url);
        if(!$data){
            return $result;
        }
        $list = $data->data($name);
        if(!$list){
            return $result;
        }
        $is_found = false;
        foreach($uuid as $uuid_nr => $uuid_value){
            $result[$uuid_value] = false;
            foreach($list as $nr => $record){
                if(
                    is_object($record) &&
                    property_exists($record, 'uuid')
                ){
                    if($record->uuid === $uuid_value){
                        $is_found = true;
                        unset($uuid[$uuid_nr]);
                        if(is_array($list)){
                            unset($list[$nr]);
                        }
                        elseif(is_object($list)){
                            unset($list->{$nr});
                        }
                        $result[$uuid_value] = true;
                        if(
                            array_key_exists('event', $options) &&
                            $options['event'] === true
                        ){
                            Event::trigger($object, 'raxon.org.node.delete', [
                                'class' => $name,
                                'node' => $record,
                                'options' => $options,
                                'role' => $role
                            ]);
                        }
                    }
                }
            }
        }
        if($is_found){
            if(empty($list)){
                File::delete($url);
            } else {
                if(is_object($list)){
                    $list = (array) $list;
                }
                $list = array_values($list);
                $data->data($name, $list);
                $data->write($url);
            }
        }
        return $result;
    }
}