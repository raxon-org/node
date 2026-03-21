<?php

namespace Package\Raxon\Node\Controller;

use Package\Raxon\Account\Module\Permission;
use Raxon\App;


use Raxon\Module\Controller;
use Raxon\Module\Dir;
use Raxon\Module\Response;
use Raxon\Node\Module\Node as Model;

use Exception;

use Raxon\Exception\ObjectException;
use Raxon\Exception\FileWriteException;

class Node extends Controller {
    const DIR = __DIR__ . '/';

    public static function object_list(App $object)
    {
        //need user role
        ddd($object->request());
    }


    /**
     * @throws ObjectException
     * @throws FileWriteException
     * @throws Exception
     */
    public static function list(App $object): Response
    {
        $role = Permission::controller($object, $object->request('class'), __FUNCTION__, $user);
        if(empty($role)) {
            throw new Exception('Role is empty...');
        }
        $model = new Model($object);
        $sort = $object->request('sort');
        if(empty($sort)){
            $sort = [
                'uuid' => 'ASC'
            ];
        }
        $filter = $object->request('filter');
        if(empty($filter)){
            $filter = [];
        }
        elseif(!is_array($filter)){
            throw new Exception('Filter must be an array.');
        }
        $limit = (int) $object->request('limit');
        if(empty($limit)){
            $limit = 30;
        }
        $page = (int) $object->request('page');
        if(empty($page)){
            $page = 1;
        }
        if(
            property_exists($role, 'name') &&
            $role->name === 'ROLE_USER'
        ){
            $filter['user'] = $user->uuid;
        }
        $output = $object->request('output');
        d($output);
        $response = $model->list(
            $object->request('class'),
            $role,
            [
                'sort' => $sort,
                'filter' => $filter,
                'limit' =>  $limit,
                'page' => $page
            ]
        );
        return new Response(
            $response,
            Response::TYPE_JSON
        );
    }

    public static function getRelation(){
        $debug = debug_backtrace(1);
        d($debug[0]['file'] . ' ' . $debug[0]['line'] . ' ' . $debug[0]['function']);
        d($debug[1]['file'] . ' ' . $debug[1]['line'] . ' ' . $debug[1]['function']);
//        d($debug[2]['file'] . ' ' . $debug[2]['line'] . ' ' . $debug[2]['function']);
    }

    /**
     * @throws Exception
     */
    public static function object_tree(App $object): Response
    {
        $dir = new Dir();
        $read = $dir->read(
            $object->config('project.dir.data') .
            'Node' .
            $object->config('ds') .
            'Object' .
            $object->config('ds'),
            false
        );
        $data = [];
        $data['nodeList'] = [];
        $data['nodeList']['tree'] = $read;
        return new Response(
            $data,
            Response::TYPE_JSON
        );
    }

}