<?php

namespace Raxon\Org\Node\Trait\Data;

use Raxon\Org\App;

use Raxon\Org\Module\Controller;
use Raxon\Org\Module\Core;
use Raxon\Org\Module\Data as Storage;
use Raxon\Org\Module\File;
use Raxon\Org\Module\Sort;

use Raxon\Org\Node\Service\Security;

use Exception;

trait Page {

    /**
     * @throws Exception
     */
    public function page($class, $role, $options=[]): false | int
    {
        $name = Controller::name($class);
        $options = Core::object($options, Core::OBJECT_ARRAY);
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        if(!array_key_exists('relation', $options)){
            $options['relation'] = false;
        }
        if(!array_key_exists('parse', $options)){
            $options['parse'] = false;
        }
        if(!array_key_exists('uuid', $options)){
            return false;
        }
        if(!Security::is_granted(
            $name,
            $role,
            $options
        )){
            return false;
        }
        $object = $this->object();
        $data_url = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds') .
            $name .
            $object->config('extension.json')
        ;
        if(!File::exist($data_url)){
            return false;
        }
        $data = $object->data_read($data_url);
        $mtime = File::mtime($data_url);
        $object_url = $object->config('project.dir.node') .
            'Object' .
            $object->config('ds') .
            $name .
            $object->config('extension.json')
        ;
        $object_data = $object->data_read($object_url);
        if($data){
            $list = $data->data($name);
            if(
                !empty($list) &&
                is_array($list)
            ){
                $max = count($list);
                $relation = [];
                if($object_data){
                    $relation = $object_data->get('relation');
                }
                if(!empty($relation) && is_array($relation)){
                    ddd('has relation');
                }
                $is_filter = false;
                $is_where = false;
                if(
                    !empty(
                    $options['filter']) &&
                    is_array($options['filter'])
                ){
                    $is_filter = true;
                }
                elseif(
                    !empty($options['where']) &&
                    (
                        is_string($options['where']) ||
                        is_array($options['where'])
                    )
                ){
                    $options['where'] = $this->where_convert($options['where']);
                    $is_where = true;
                }
                foreach($list as $nr => $record) {
                    if(is_object($record)){
                        $expose = $this->expose_get(
                            $object,
                            $record->{'#class'},
                            $record->{'#class'} . '.' . $options['function'] . '.output'
                        );
                        $node = new Storage($record);
                        $node = $this->expose(
                            $node,
                            $expose,
                            $record->{'#class'},
                            $options['function'],
                            $role
                        );
                        $record = $node->data();
                    }
                    //parse the record if parse is enabled
                    if($is_filter){
                        $record = $this->filter($record, $options['filter'], $options);
                        if(!$record){
                            unset($list[$nr]);
                        }
                    }
                    elseif($is_where){
                        $record = $this->where($record, $options['where'], $options);
                        if(!$record){
                            unset($list[$nr]);
                        }
                    }
                }
                $list = array_values($list);
                $limit = $options['limit'] ?? 4096;
                if(
                    !empty($options['sort']) &&
                    is_array($options['sort']) &&
                    $limit !== 1
                ){
                    $list = Sort::list($list)->with(
                        $options['sort'],
                        [
                            'key_reset' => true,
                        ]
                    );
                }
                $limit = $options['limit'] ?? 4096;
                $counter = 0;
                $page = 1;
                $is_found = false;
                foreach($list as $index => $record){
                    if(
                        is_object($record) &&
                        property_exists($record, 'uuid') &&
                        $record->uuid === $options['uuid']
                    ){
                        $is_found = true;
                        break;
                    }
                    if($counter >= $limit){
                        $page++;
                        $counter = 0;
                    }
                    $counter++;
                }
                if($is_found){
                    return $page;
                }
            }
        }
        return false;
    }
}