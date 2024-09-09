<?php

namespace Raxon\Node\Trait\Data;

use Raxon\Module\Cli;
use Raxon\Module\Controller;
use Raxon\Module\Core;
use Raxon\Module\Data as Storage;

use Raxon\Module\Event;
use Raxon\Node\Service\Security;

use Exception;

trait Put {

    /**
     * @throws Exception
     */
    public function put($class, $role, $node=[], $options=[]): false | array
    {
        $options = Core::object($options, Core::OBJECT_ARRAY);
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        $nodeList = [$node];
        $response = $this->put_many($class, $role, $nodeList, $options);
        return $this->single($response);
    }

    /**
     * @throws Exception
     */
    public function put_many($class, $role, $nodeList=[], $options=[]): false | array
    {
        $name = Controller::name($class);
        $object = $this->object();
        $options = Core::object($options, Core::OBJECT_ARRAY);
        $start = false;
        if(
            array_key_exists('duration', $options) &&
            $options['duration'] === true
        ){
            $start = microtime(true);
        }
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        if(!array_key_exists('import', $options)){
            $options['import'] = false;
        }
        if(!array_key_exists('uuid', $options)){
            $options['uuid'] = false;
        }
        if(!array_key_exists('relation', $options)){
            $options['relation'] = false;
        }
        if(!array_key_exists('transaction', $options)){
            $options['transaction'] = false;
        }
        if(!array_key_exists('memory', $options)){
            $options['memory'] = false;
        }
        $relation = $options['relation'];
        $options['relation'] = false; //first put without relation
        if(!Security::is_granted(
            $name,
            $role,
            $options
        )){
            return false;
        }
        $transaction = $object->config('node.transaction.' . $name);
        if(
            $options['import'] === false &&
            empty($transaction)
        ){
            $this->lock($name, $options);
        }
        $dir_data = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds')
        ;
        $url = $dir_data .
            $name .
            $object->config('extension.json')
        ;
        $dir_validate = $object->config('project.dir.node') .
            'Validate'.
            $object->config('ds')
        ;
        $validate_url =
            $dir_validate .
            $name .
            $object->config('extension.json');

        $start = microtime(true);
        if($transaction === true){
            $data = $object->data_read($url, sha1($url));
        } else {
            $data = $object->data_read($url);
        }
        if($data){
            $list = $data->get($name);
            if(!is_array($list)){
                throw new Exception('Array expected');
            }
        } else {
            $list = [];
        }
        $uuid = [];
        foreach($list as $nr => $record){
            $uuid[$record->uuid] = $nr;
        }
        $error = [];
        $result = [];
        $object_data = null;
        $count = 0;
        foreach($nodeList as $nr => $node){
            if(
                is_object($node) &&
                get_class($node) === Storage::class
            ){
                $node = $node->data();
            } else {
                $node = Core::object($node, Core::OBJECT_OBJECT);
            }
            $object->request('node', $node);
            $object->request('node.#class', $name);
            if(
                array_key_exists('validation', $options) &&
                $options['validation'] === false
            ){
                $validate = (object) ['success' => true];
            } else {
                $validate = $this->validate($object, $validate_url,  $name . '.put', $options['function']);
            }
            if($validate) {
                if ($validate->success === true) {
                    $expose = $this->expose_get(
                        $object,
                        $name,
                        $name . '.' . $options['function'] . '.output'
                    );
                    $node = new Storage();
                    $node->data($object->request('node'));
                    $node->set('#class', $name);
                    if (
                        $expose &&
                        $role
                    ) {
                        $node = $this->expose(
                            $node,
                            $expose,
                            $name,
                            $options['function'],
                            $role
                        );
                        $record = $node->data();
                        if(Core::object_is_empty($record)){
                            throw new Exception('Empty node after expose...');
                        }
                        if(property_exists($record, 'uuid')){
                            if(array_key_exists($record->uuid, $uuid)){
                                $list_nr = $uuid[$record->uuid];
                                if($relation === true){
                                    $list[$list_nr] = clone $record; //don't need relation
                                    $options['relation'] = true;
                                    if(!$object_data){
                                        $object_url = $object->config('project.dir.node') .
                                            'Object' .
                                            $object->config('ds') .
                                            $name .
                                            $object->config('extension.json')
                                        ;
                                        if (
                                            $options['transaction'] === true ||
                                            $options['memory'] === true
                                        ) {
                                            $object_data = $object->data_read($object_url, sha1($object_url));
                                        } else {
                                            $object_data = $object->data_read($object_url);
                                        }
                                    }
                                    //need to update node here so it gets a relation.
                                    $record = $this->relation($record, $object_data, $role, $options);
                                    //collect relation mtime
                                } else {
                                    $list[$list_nr] = $record;
                                }
                                if(
                                    array_key_exists('event', $options) &&
                                    $options['event'] === true
                                ){
                                    Event::trigger($object, 'raxon.org.node.put', [
                                        'class' => $name,
                                        'node' => $record,
                                        'options' => $options,
                                        'role' => $role
                                    ]);
                                }
                                if(
                                    array_key_exists('function', $options) &&
                                    $options['function'] === __FUNCTION__
                                ){
                                    $result[] = $record->uuid;
                                } else {
                                    $result[] = $record;
                                }
                                if($options['import'] === true){
                                    $number = $object->config('raxon.org.node.import.list.number');
                                    if(empty($number)){
                                        $number = 1;
                                    } else {
                                        $number++;
                                    }
                                    $object->config('raxon.org.node.import.list.number', $number);
                                    $amount = $object->config('raxon.org.node.import.list.count');
                                    if($amount > 0){
                                        if($number === 1){
                                            echo 'Imported (PUT) ' . $number . ' of ' . $amount . ' nodes ('. round(($number / $amount) * 100 , 2) .' %)...' . PHP_EOL;
                                        }
                                        elseif($number % 10 === 0){
                                            if($number > 1){
                                                echo Cli::tput('cursor.up');
                                                echo str_repeat(' ', Cli::tput('columns')) . PHP_EOL;
                                                echo Cli::tput('cursor.up');
                                            }
                                            echo 'Imported (PUT) ' . $number . ' of ' . $amount . ' nodes ('. round(($number / $amount) * 100 , 2) .' %)...' . PHP_EOL;
                                        }
                                        elseif($number === $amount){
                                            echo Cli::tput('cursor.up');
                                            echo str_repeat(' ', Cli::tput('columns')) . PHP_EOL;
                                            echo Cli::tput('cursor.up');
                                            echo 'Imported (PUT) ' . $number . ' of ' . $amount . ' nodes ('. round(($number / $amount) * 100 , 2) .' %)...' . PHP_EOL;
                                        }
                                    }
                                }
                                $count++;
                            }
                        }
                    }
                } else {
                    $error[] = $validate->test;
                }
            }
        }
        $object->config('delete', 'raxon.org.node.import.list');
        if(!empty($error)){
            $response = [];
            $response['error'] = $error;
            if(
                $options['import'] === false &&
                empty($transaction)
            ){
                $this->unlock($name);
            }
            return $response;
        }
        $data->set($name, $list);
        $response = [];
        $response['list'] = $result;
        $response['count'] = $count;
        if($transaction === true){
            $object->data(sha1($url), $data);
            $response['transaction'] = true;
        } else {
            $write = $data->write($url);
            $response['byte'] = $write;
            $response['transaction'] = false;
            if($options['import'] === false){
                $this->unlock($class);
            }
        }
        if($start){
            $response['duration'] = (object) [
                'boot' => ($start - $object->config('time.start')) * 1000,
                'total' => (microtime(true) - $object->config('time.start')) * 1000,
                'nodelist' => (microtime(true) - $start) * 1000
            ];
            $response['duration']->item_per_second = ($response['count'] / $response['duration']->total) * 1000;
            $response['duration']->item_per_second_nodelist = ($response['count'] / $response['duration']->nodelist) * 1000;
        }
        return $response;
    }
}